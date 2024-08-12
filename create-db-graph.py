import osmnx as ox
import neo4j
import SaintPetersburgParseData as SaintParser
import pandas as pd

constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Intersection) REQUIRE i.osmid IS UNIQUE"

rel_index_query = "CREATE INDEX IF NOT EXISTS FOR ()-[r:ROAD_SEGMENT]-() ON r.osmids"

address_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.id IS UNIQUE"

point_index_query = "CREATE POINT INDEX IF NOT EXISTS FOR (i:Intersection) ON i.location"

node_query = '''
    UNWIND $rows AS row
    WITH row WHERE row.osmid IS NOT NULL
    MERGE (s:Stop {osmid: row.osmid})
        SET s.location = 
         point({latitude: row.y, longitude: row.x }),
            s.name = row.name,
            s.highway = row.highway,
            s.public_transport = row.public_transport,
            s.routes = row.routes,
            s.tram = row.tram,
            s.bus = row.bus,
            s.geometry_wkt = row.geometry_wkt,
            s.street_count = toInteger(row.street_count)
    RETURN COUNT(*) as total
    '''

rels_query = '''
    UNWIND $rows AS path
    MATCH (u:Stop {osmid: path.u})
    MATCH (v:Stop {osmid: path.v})
    MERGE (u)-[r:ROUTE_SEGMENT {osmid: path.osmid}]->(v)
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

node_query_bus = '''
    UNWIND $rows AS row
    WITH row WHERE row.name IS NOT NULL
    MERGE (s:Stop {name: row.name})
        SET s.location = 
         point({latitude: row.yCoordinate, longitude: row.xCoordinate }),
            s.roteList = row.roteList
    RETURN COUNT(*) AS total
'''

rels_query_bus = '''
    UNWIND $rows AS path
    MATCH (u:Stop {name: path.startStop})
    MATCH (v:Stop {name: path.endStop})
    MERGE (u)-[r:ROUTE_SEGMENT {name: path.name}]->(v)
        SET r.name = path.name,
            r.duration = path.duration
    RETURN COUNT(*) AS total
'''

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "123456789"

def create_constraints(tx):
    result = tx.run(constraint_query)
    result = tx.run(rel_index_query)
    result = tx.run(address_constraint_query)
    result = tx.run(point_index_query)

def insert_data(tx, query, rows, batch_size=10000):
    total = 0
    batch = 0

    df = pd.DataFrame(rows)

    while batch * batch_size < len(df):
        current_batch = df.iloc[batch*batch_size:(batch+1)*batch_size]
        batch_data = current_batch.to_dict('records')
        data = {'rows': batch_data}
        results = tx.run(query, parameters={'rows': batch_data}).data()
        print(results)
        total += results[0]['total']
        batch += 1
    return total

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
        session.execute_write(create_constraints)
        session.execute_write(insert_data, node_query, gdf_nodes.drop(columns=["geometry"]))

    with driver.session() as session:
        session.execute_write(insert_data, rels_query, gdf_relationships.drop(columns=["geometry"]))

def create_saint_petersubrg_bus_graph_db():
    (nodes, relationships) = SaintParser.get_saint_petersburg_bus_graph()

    new_node = list(nodes.values())

    driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    driver.verify_connectivity()

    with driver.session() as session:
        session.execute_write(create_constraints)

        session.execute_write(insert_data, node_query_bus, new_node)

        session.execute_write(insert_data, rels_query_bus, relationships)

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
        session.execute_write(create_constraints)
        session.execute_write(insert_data, node_query, gdf_nodes.drop(columns=["geometry"]))

    with driver.session() as session:
        session.execute_write(insert_data, rels_query, gdf_relationships.drop(columns=["geometry"]))

if __name__ == "__main__":
    create_saint_petersubrg_bus_graph_db()