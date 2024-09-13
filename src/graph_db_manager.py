from abc import abstractmethod

import osmnx as ox
import pandas as pd

from neo4j_connection import Neo4jConnection
from parser import BusGraphParser


class GraphDBManager:
    def __init__(self, node_name, relationship_name):
        self.node_name = node_name
        self.relationship_name = relationship_name
        self.connection = Neo4jConnection()
        self.constraints = []

    def update_db(self, city_name):
        (nodes, relationships) = self.get_graph(city_name)
        if nodes is None and relationships is None:
            print("Graph for", city_name, "is empty!")
            return

        self.connection.execute_write(self.create_constraints)
        self.connection.execute_write(insert_data, self.create_node_query(), nodes)
        self.connection.execute_write(insert_data, self.create_relationships_query(), relationships)

    @abstractmethod
    def get_graph(self, city_name):
        pass

    def create_constraints(self, tx):
        constraints = self.get_constraint_list()
        for constraint in constraints:
            tx.run(constraint)

    @abstractmethod
    def get_constraint_list(self):
        pass

    @abstractmethod
    def create_node_query(self):
        pass

    @abstractmethod
    def create_relationships_query(self):
        pass


class RoadGraphDBManager(GraphDBManager):

    def get_graph(self, city_name):
        g = ox.graph_from_place(city_name, simplify=True, retain_all=True, network_type="drive")

        gdf_nodes, gdf_relationships = ox.graph_to_gdfs(g)
        gdf_nodes.reset_index(inplace=True)
        gdf_relationships.reset_index(inplace=True)
        gdf_nodes["geometry_wkt"] = gdf_nodes["geometry"].apply(lambda x: x.wkt)
        gdf_relationships["geometry_wkt"] = gdf_relationships["geometry"].apply(lambda x: x.wkt)

        return gdf_nodes.drop(columns=["geometry"]), gdf_relationships.drop(columns=["geometry"])

    def create_node_query(self):
        return f'''
        UNWIND $rows AS row
        WITH row WHERE row.osmid IS NOT NULL
        MERGE (i:{self.node_name} {{osmid: row.osmid}})
            SET i.location = point({{latitude: row.y, longitude: row.x }}),
                i.highway = row.highway,
                i.tram = row.tram,
                i.bus = row.bus,
                i.geometry_wkt = row.geometry_wkt,
                i.street_count = toInteger(row.street_count)
        RETURN COUNT(*) as total
        '''

    def create_relationships_query(self):
        return f'''
        UNWIND $rows AS path
        MATCH (u:{self.node_name} {{osmid: path.u}})
        MATCH (v:{self.node_name} {{osmid: path.v}})
        MERGE (u)-[r:{self.relationship_name} {{osmid: path.osmid}}]->(v)
            SET r.name = path.name,
                r.highway = path.highway,
                r.railway = path.railway,
                r.oneway = path.oneway,
                r.lanes = path.lanes,
                r.max_speed = path.maxspeed,
                r.geometry_wkt = path.geometry_wkt,
                r.length = toFloat(path.length)
        RETURN COUNT(*) AS total
        '''

    def get_constraint_list(self):
        return [
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (i:{self.node_name}) REQUIRE i.osmid IS UNIQUE",
            f"CREATE INDEX IF NOT EXISTS FOR ()-[r:{self.relationship_name}]-() ON r.osmid"
        ]


class BusGraphDBManager(GraphDBManager):

    def get_graph(self, city_name):
        parser = BusGraphParser(city_name)
        (nodes, relationships) = parser.parse()
        return list(nodes.values()), relationships

    def create_node_query(self):
        return f'''
            UNWIND $rows AS row
            WITH row WHERE row.name IS NOT NULL
            MERGE (s:{self.node_name} {{name: row.name}})
                SET s.location = point({{latitude: row.yCoordinate, longitude: row.xCoordinate }}),
                    s.roteList = row.roteList,
                    s.isCoordinateApproximate = row.isCoordinateApproximate
            RETURN COUNT(*) AS total
        '''

    def create_relationships_query(self):
        return f'''
            UNWIND $rows AS path
            MATCH (u:{self.node_name} {{name: path.startStop}})
            MATCH (v:{self.node_name} {{name: path.endStop}})
            MERGE (u)-[r:{self.relationship_name} {{name: path.name}}]->(v)
                SET r.duration = path.duration
            RETURN COUNT(*) AS total
        '''

    def get_constraint_list(self):
        return [
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (s:{self.node_name}) REQUIRE s.name IS UNIQUE",
            f"CREATE INDEX IF NOT EXISTS FOR ()-[r:{self.relationship_name}]-() ON r.name"
        ]


def insert_data(tx, query, rows, batch_size=10000):
    total = 0
    batch = 0

    df = pd.DataFrame(rows)

    while batch * batch_size < len(df):
        current_batch = df.iloc[batch * batch_size:(batch + 1) * batch_size]
        batch_data = current_batch.to_dict('records')
        results = tx.run(query, parameters={'rows': batch_data}).data()
        print(results)
        total += results[0]['total']
        batch += 1
    return total
