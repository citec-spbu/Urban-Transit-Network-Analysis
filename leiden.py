import Neo4jConnection as ConnectionFactory

made_leiden_graph = '''
    CALL gds.graph.project(
    $name,
    'Stop',
    {
        ROUTE_SEGMENT: {
            orientation: 'UNDIRECTED',
            properties: 'duration'
        }
    }
)
    '''

community_leiden_graph = '''
        CALL gds.leiden.stream(
            $name,
            {
                relationshipWeightProperty: 'duration'
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

connection = ConnectionFactory.Neo4jConnection()


def leiden_cluster(graph_name):
    params = {"name": graph_name}
    connection.query(made_leiden_graph, params)
    connection.query(community_leiden_graph, params)
    connection.query(write_leiden_graph, params)


if __name__ == "__main__":
    leiden_cluster("leidenAlgorithmGraph")
