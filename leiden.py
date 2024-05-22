import Neo4jConnection as con

made_leiden_graph = '''
    CALL gds.graph.project(
    $name,
    'Stop',
    {
        ROUTE_SEGMENT: {
            orientation: 'UNDIRECTED',
            properties: 'length'
        }
    }
)
    '''

community_leiden_graph = '''
        CALL gds.leiden.stream(
            $name,
            {
                relationshipWeightProperty: 'length'
            }
        )
        YIELD nodeId, communityId
        RETURN communityId, COUNT(DISTINCT nodeId) AS members
        ORDER BY members DESC
    '''

write_leiden_graph = '''
       CALL gds.leiden.write(
           $name, 
               {
                   writeProperty: 'leiden_community'
               }
        ) 
        YIELD communityCount, modularity, modularities
    '''

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "123456789"
conn = con.Neo4jConnection(uri=NEO4J_URI, user=NEO4J_USER, pwd=NEO4J_PASSWORD)

def leiden_cluster(graph_name):
    conn.query(made_leiden_graph, {"name": graph_name})

    conn.query(community_leiden_graph, {"name": graph_name})

    conn.query(write_leiden_graph, {"name": graph_name})

leiden_cluster("leidenAlgoritmGraph")