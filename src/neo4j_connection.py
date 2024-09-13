from neo4j import GraphDatabase


class Neo4jConnection:

    def __init__(self, uri="bolt://localhost:7687", user="neo4j", pwd="123456789"):

        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None

        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):

        if self.__driver is not None:
            self.__driver.close()

    # TODO: need to add decorator for run and execute_write
    def run(self, query, parameters=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None

        try:
            session = self.__driver.session()
            result = session.run(query, parameters)
            print(list(result))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()

    def execute_write(self, transaction_function, *args):
        assert self.__driver is not None, "Driver not initialized!"
        session = None

        try:
            session = self.__driver.session()
            session.execute_write(transaction_function, *args)
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
