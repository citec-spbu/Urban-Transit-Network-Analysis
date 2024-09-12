import create_db_graph
import leiden
import louvain

if __name__ == "__main__":
    city_name = "Керчь"
    create_db_graph.create_graph_db(city_name)
    create_db_graph.create_bus_graph_db(city_name)
    leiden.leiden_cluster("leidenAlgorithmGraph")
    louvain.louvain_clustering("louvainAlgorithmGraph")
