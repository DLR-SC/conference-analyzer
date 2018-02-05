from neo4j.v1 import GraphDatabase
import DataModels

class insert_data(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def insert_data(self, person_name, conference, conference_series, talk, topic, track=False):
        with self._driver.session() as session:
            session.write_transaction(self.create_nodes_f, person_name, conference, talk)
            if(conference.part_of_series):
                session.write_transaction(self.conference_series_f, conference, conference_series)
            session.write_transaction(self.person_attends_conference_f, person_name, conference)
            session.write_transaction(self.person_presents_talk_f, person_name, talk)
            session.write_transaction(self.conference_has_talk_f, conference, talk)
            session.write_transaction(self.topic_f, talk, topic)
    def create_nodes(self, name, conference):
        with self._driver.session() as session:
            result = session.write_transaction(self.create_nodes_f, name, conference)
    def person_attends_conference(self, name, conference):
        with self._driver.session() as session:
            session.write_transaction(self.person_attends_conference_f, name, conference)

    @staticmethod
    def person_attends_conference_f(tx, person_name, conference):
        tx.run("MERGE (c:Conference {name: $conferenceName, location: $conferenceLocation, date: $conferenceDate}) MERGE (p:Person{name: $name}) "
                "MERGE (p)-[:`attends` ]->(c)", parameters={"conferenceName" : conference.name, 
                "conferenceLocation" : conference.location, "conferenceDate" : conference.date, "name" : person_name})
    @staticmethod
    def person_presents_talk_f(tx, person_name, talk):
        tx.run("MERGE (t:Talk {title: $title, language: $language}) MERGE (p:Person{name: $name}) "
                "MERGE (p)-[:`presents` ]->(t)", parameters={"title" : talk.title, 
                "language" : talk.language, "name" : person_name})
    @staticmethod
    def conference_has_talk_f(tx, conference, talk, track=False):
        if(type(track) is str):
            tx.run("MERGE (t:Talk {title: $title, language: $language})"
                    "MERGE (c:Conference {name: $conferenceName, location: $conferenceLocation, date: $conferenceDate}) "
                    "MERGE (c)-[:`has` {track: $Default}]->(t)", parameters={"Default" : track, "title" : talk.title,
                    "language" : talk.language, "conferenceName" : conference.name, "conferenceLocation" : conference.location,
                    "conferenceDate" : conference.date})
        else:    
            tx.run("MERGE (t:Talk {title: $title, language: $language})"
                    "MERGE (c:Conference {name: $conferenceName, location: $conferenceLocation, date: $conferenceDate}) "
                    "MERGE (c)-[:`has` {track: $Default}]->(t)", parameters={"Default" : "default", "title" : talk.title,
                    "language" : talk.language, "conferenceName" : conference.name, "conferenceLocation" : conference.location,
                    "conferenceDate" : conference.date})
    @staticmethod
    def topic_f(tx, talk, topic):
        tx.run("MERGE (to:Topic{topic_name: $topic_name, tags: $tags}) MERGE (t:Talk{title: $title, language: $language}) "
                "MERGE (t)-[:`covers`]->(to)", parameters={"topic_name" : topic.name, "tags" : topic.tags, "title" : talk.title,
                "language" : talk.language})
    @staticmethod
    def conference_series_f(tx, conference, conference_series):
        tx.run("MERGE (cs:ConferenceSeries{name: $conferenceSeriesName, frequency: $frequency} ) "
                "MERGE (c:Conference{name: $conferenceName, location: $conferenceLocation, date: $conferenceDate}) "
                "MERGE (c)-[:`partOf`]->(cs)",
                parameters={"conferenceSeriesName" : conference_series.name, "frequency" : conference_series.frequency,
                "conferenceName" : conference.name, "conferenceLocation" : conference.location, "conferenceDate" : conference.date})
    @staticmethod
    def create_nodes_f(tx, person_name, conference, talk):
        result = tx.run("MERGE (c:Conference {name: $conferenceName, location: $conferenceLocation, date: $conferenceDate}) "
                        "MERGE (person:Person {name: $name}) "
                        "MERGE (t:Talk {title: $title, language: $language})"
                        , parameters={"conferenceName" : conference.name, "conferenceLocation" : conference.location,
                        "conferenceDate" : conference.date, "name" : person_name, "title" : talk.title, "language" : talk.language})