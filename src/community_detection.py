from neo4j_connection import Neo4jConnection


class CommunityDetection:
    def __init__(self, algorithm_name, property_name):
        self.algorithm_name = algorithm_name
        self.property_name = property_name
        self.connection = Neo4jConnection()

    def detect_communities(self, graph_name, relationship_weight_property, node_name, relationship_name):
        self.__make_graph(graph_name, node_name, relationship_name, relationship_weight_property)
        self.__detect_communities(graph_name, relationship_weight_property)
        self.__write_communities(graph_name)

    def __make_graph(self, graph_name, node_name, relationship_name, relationship_weight_property):
        query = f'''
            CALL gds.graph.project(
            '{graph_name}',
            '{node_name}',
            {{
                {relationship_name}: {{
                    orientation: 'UNDIRECTED',
                    properties: '{relationship_weight_property}'
                }}
            }}
        )
        '''
        self.connection.run(query)

    def __detect_communities(self, graph_name, relationship_weight_property):
        query = f'''
            CALL gds.{self.algorithm_name}.stream(
                '{graph_name}',
                {{
                    relationshipWeightProperty: '{relationship_weight_property}'
                }}
            )
            YIELD nodeId, communityId
            RETURN communityId, COUNT(DISTINCT nodeId) AS members
            ORDER BY members DESC
        '''
        return self.connection.run(query)

    def __write_communities(self, graph_name):
        query = f'''
            CALL gds.{self.algorithm_name}.write(
                '{graph_name}', 
                {{
                    writeProperty: '{self.property_name}'
                }}
            ) 
            YIELD communityCount, modularity, modularities
        '''
        return self.connection.run(query)


class Leiden(CommunityDetection):
    def __init__(self):
        super().__init__("leiden", "leiden_community")


class Louvain(CommunityDetection):
    def __init__(self):
        super().__init__("louvain", "louvain_community")
