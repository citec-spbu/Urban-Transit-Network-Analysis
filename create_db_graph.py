import osmnx as ox
import neo4j
import ParseData
import pandas as pd

road_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Intersection) REQUIRE i.osmid IS UNIQUE"
road_index_query = "CREATE INDEX IF NOT EXISTS FOR ()-[r:RoadSegment]-() ON r.osmid"

bus_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Stop) REQUIRE s.name IS UNIQUE"
bus_index_query = "CREATE INDEX IF NOT EXISTS FOR ()-[r:RouteSegment]-() ON r.name"

road_node_query = '''
    UNWIND $rows AS row
    WITH row WHERE row.osmid IS NOT NULL
    MERGE (i:Intersection {osmid: row.osmid})
        SET i.location = point({latitude: row.y, longitude: row.x }),
            i.highway = row.highway,
            i.tram = row.tram,
            i.bus = row.bus,
            i.geometry_wkt = row.geometry_wkt,
            i.street_count = toInteger(row.street_count)
    RETURN COUNT(*) as total
    '''

road_rels_query = '''
    UNWIND $rows AS path
    MATCH (u:Intersection {osmid: path.u})
    MATCH (v:Intersection {osmid: path.v})
    MERGE (u)-[r:RoadSegment {osmid: path.osmid}]->(v)
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

bus_node_query = '''
    UNWIND $rows AS row
    WITH row WHERE row.name IS NOT NULL
    MERGE (s:Stop {name: row.name})
        SET s.location = point({latitude: row.yCoordinate, longitude: row.xCoordinate }),
            s.roteList = row.roteList,
            s.isCoordinateApproximate = row.isCoordinateApproximate
    RETURN COUNT(*) AS total
'''

bus_rels_query = '''
    UNWIND $rows AS path
    MATCH (u:Stop {name: path.startStop})
    MATCH (v:Stop {name: path.endStop})
    MERGE (u)-[r:RouteSegment {name: path.name}]->(v)
        SET r.duration = path.duration
    RETURN COUNT(*) AS total
'''

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "123456789"


def create_road_constraints(tx):
    tx.run(road_constraint_query)
    tx.run(road_index_query)


def create_bus_constraints(tx):
    tx.run(bus_constraint_query)
    tx.run(bus_index_query)


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


def create_bus_graph_db(city_name):
    (nodes, relationships) = ParseData.get_bus_graph(city_name)
    if nodes is None and relationships is None:
        return

    new_nodes = list(nodes.values())

    driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()

    with driver.session() as session:
        session.execute_write(create_bus_constraints)
        session.execute_write(insert_data, bus_node_query, new_nodes)
        session.execute_write(insert_data, bus_rels_query, relationships)


def create_graph_db(city_name):
    g = ox.graph_from_place(city_name, simplify=True, retain_all=True, network_type="drive")

    gdf_nodes, gdf_relationships = ox.graph_to_gdfs(g)
    gdf_nodes.reset_index(inplace=True)
    gdf_relationships.reset_index(inplace=True)
    gdf_nodes["geometry_wkt"] = gdf_nodes["geometry"].apply(lambda x: x.wkt)
    gdf_relationships["geometry_wkt"] = gdf_relationships["geometry"].apply(lambda x: x.wkt)

    driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()

    with driver.session() as session:
        session.execute_write(create_road_constraints)
        session.execute_write(insert_data, road_node_query, gdf_nodes.drop(columns=["geometry"]))

    with driver.session() as session:
        session.execute_write(insert_data, road_rels_query, gdf_relationships.drop(columns=["geometry"]))


if __name__ == "__main__":
    city = "Керчь"
    create_graph_db(city)
    create_bus_graph_db(city)
