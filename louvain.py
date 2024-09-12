import Neo4jConnection as ConnectionFactory

made_louvain_graph = '''
    CALL gds.graph.project(
    $name,
    'Stop',
    {
        RouteSegment: {
            orientation: 'UNDIRECTED',
            properties: 'length'
        }
    }
)
    '''

community_louvain_graph = '''
        CALL gds.louvain.stream(
            $name,
            {
                relationshipWeightProperty: 'length'
            }
        )
        YIELD nodeId, communityId
        RETURN communityId, COUNT(DISTINCT nodeId) AS members
        ORDER BY members DESC
    '''

write_louvain_graph = '''
       CALL gds.louvain.write(
           $name, 
               {
                   writeProperty: 'louvain_community'
               }
        ) 
        YIELD communityCount, modularity, modularities
    '''

connection = ConnectionFactory.Neo4jConnection()


def louvain_clustering(graph_name):
    params = {"name": graph_name}
    connection.query(made_louvain_graph, params)
    connection.query(community_louvain_graph, params)
    connection.query(write_louvain_graph, params)


if __name__ == "__main__":
    louvain_clustering("louvainAlgorithmGraph")
