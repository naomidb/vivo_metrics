from datetime import datetime
import requests
import sqlite3

from text_writer import write_to_db
from text_writer import write_to_csv

def run(query):
    email = 'null'
    password = 'null'
    endpoint = 'https://your.query.endpoint'

    print("Query:\n" + query)
    payload = {
        'email': email,
        'password': password,
        'query': query,
    }
    headers = {'Accept': 'application/sparql-results+json'}
    response = requests.get(endpoint, params=payload, headers=headers, verify=False)
    print(response)
    return response.json()

def get_author_count():
    q = '''SELECT (count(distinct ?author_uri) as ?count)
            WHERE { ?author_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
                    ?author_uri <http://vivoweb.org/ontology/core#relatedBy> ?relationship .
                    ?relationship <http://vivoweb.org/ontology/core#relates> ?article_uri .
                    ?article_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_gatorlink_count():
    q = '''SELECT (count(distinct ?author_uri) as ?count)
            WHERE { ?author_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
                    ?author_uri <http://vivoweb.org/ontology/core#relatedBy> ?relationship .
                    ?author_uri <http://vivo.ufl.edu/ontology/vivo-ufl/gatorlink> ?gatorlink .
                    ?relationship <http://vivoweb.org/ontology/core#relates> ?article_uri .
                    ?article_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_ufentity_count():
    q = '''SELECT (count(distinct ?author_uri) as ?count)
            WHERE { ?author_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
                    ?author_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivo.ufl.edu/ontology/vivo-ufl/UFEntity> .
                    ?author_uri <http://vivoweb.org/ontology/core#relatedBy> ?relationship .
                    ?relationship <http://vivoweb.org/ontology/core#relates> ?article_uri .
                    ?article_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_ufcurrententity_count():
    q = '''SELECT (count(distinct ?author_uri) as ?count)
            WHERE { ?author_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
                    ?author_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivo.ufl.edu/ontology/vivo-ufl/UFCurrentEntity> .
                    ?author_uri <http://vivoweb.org/ontology/core#relatedBy> ?relationship .
                    ?relationship <http://vivoweb.org/ontology/core#relates> ?article_uri .
                    ?article_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_publisher_count():
    q = '''SELECT (count(distinct ?publisher_uri) as ?count)
            WHERE { ?publisher_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Publisher> . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_journal_count():
    q = '''SELECT (count(distinct ?journal_uri) as ?count)
            WHERE { ?journal_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Journal> . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_publication_count():
    q = '''SELECT (count(distinct ?pub_uri) as ?count)
            WHERE { ?pub_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_doi_count():
    q = '''SELECT (count(distinct ?pub_uri) as ?count)
            WHERE { ?pub_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
                    ?pub_uri <http://purl.org/ontology/bibo/doi> ?doi . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_pmid_count():
    q = '''SELECT (count(distinct ?pub_uri) as ?count)
            WHERE { ?pub_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
                    ?pub_uri <http://purl.org/ontology/bibo/pmid> ?pmid . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_doi_pmid_count():
    q = '''SELECT (count(distinct ?pub_uri) as ?count)
            WHERE { ?pub_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
                    ?pub_uri <http://purl.org/ontology/bibo/doi> ?doi .
                    ?pub_uri <http://purl.org/ontology/bibo/pmid> ?pmid . }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def get_orphan_pubs():
    q = '''SELECT (count(distinct ?pub_uri) as ?count)
            WHERE { ?pub_uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> . 
            FILTER NOT EXISTS { ?pub_uri <http://vivoweb.org/ontology/core#relatedBy> ?relationship . }
            }'''
    response = run(q)
    count = response['results']['bindings'][0]['count']['value']
    return count

def main():
    numbers = {
        'author_count': get_author_count(),
        'gatorlink_count': get_gatorlink_count(),
        'ufentity_count': get_ufentity_count(),
        'ufcurrententity_count': get_ufcurrententity_count(),
        'publisher_count': get_publisher_count(),
        'journal_count': get_journal_count(),
        'publication_count': get_publication_count(),
        'doi_count': get_doi_count(),
        'pmid_count': get_pmid_count(),
        'doi_pmid_count': get_doi_pmid_count(),
        'orphan_count': get_orphan_pubs()
    }

    write_to_db(**numbers)
    write_to_csv()

if __name__ == '__main__':
    main()