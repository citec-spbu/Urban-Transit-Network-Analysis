from community_detection import Leiden, Louvain
from graph_db_manager import RoadGraphDBManager, BusGraphDBManager

if __name__ == "__main__":
    city_name = "Керчь"
    all_types = [
        {"name": "Road", "nodeName": "Intersection", "relationshipName": "RoadSegment",
         "dbManagerConstructor": RoadGraphDBManager},
        {"name": "Bus", "nodeName": "Stop", "relationshipName": "RouteSegment",
         "dbManagerConstructor": BusGraphDBManager}
    ]

    leiden = Leiden()
    louvain = Louvain()

    for type_graph in all_types:
        name = type_graph["name"]
        node_name = type_graph["nodeName"]
        relationship_name = type_graph["relationshipName"]
        db_manager_constructor = type_graph["dbManagerConstructor"]

        db_manager = db_manager_constructor(node_name, relationship_name)
        db_manager.update_db(city_name)

        # TODO: road graph not have duration parameter
        leiden.detect_communities(f"{name}LeidenAlgorithmGraph", "duration", node_name, relationship_name)
        louvain.detect_communities(f"{name}LouvainAlgorithmGraph", "length", node_name, relationship_name)
        print(f"Community detection for graph {name} completed.")
