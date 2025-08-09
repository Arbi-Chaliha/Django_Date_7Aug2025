import pandas as pd
import rdflib
from rdflib import Graph, Literal, Namespace, RDF,RDFS,URIRef
from rdflib.namespace import OWL, RDF, RDFS, FOAF, XSD, DC, SKOS
from tqdm import tqdm
import requests
import os
from dotenv import load_dotenv

df = pd.read_excel("flow_manager_ontology_poc_prep.xlsx", header=1, usecols=range(1, 7))
print(df.head())

file_path = 'my_ontology.ttl'
g = Graph()
g.parse(file_path, format='turtle')

query = """
    PREFIX arg: <http://spinrdf.org/arg#>
    PREFIX dash: <http://datashapes.org/dash#>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX edg: <http://edg.topbraid.solutions/model/>
    PREFIX graphql: <http://datashapes.org/graphql#>
    PREFIX metadata: <http://topbraid.org/metadata#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX sh: <http://www.w3.org/ns/shacl#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
    PREFIX smf: <http://topbraid.org/sparqlmotionfunctions#>
    PREFIX spl: <http://spinrdf.org/spl#>
    PREFIX swa: <http://topbraid.org/swa#>
    PREFIX teamwork: <http://topbraid.org/teamwork#>
    PREFIX teamworkconstraints: <http://topbraid.org/teamworkconstraints#>
    PREFIX tosh: <http://topbraid.org/tosh#>
    PREFIX troubleshooting_ora_fnfm_ontology_: <http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT ?entity ?relation ?next_entity
    WHERE {
        ?entity sh:property ?property;
                rdfs:subClassOf owl:Thing.
        ?property sh:class ?next_entity;
                sh:path ?relation.
        
    }
"""
#This code performs a SPARQL query over an RDF graph (g) to extract:
#An entity (a class),A relation (a property path),A next entity (the target class the property points to)
result = g.query(query)
labels_list = [(str(row[0]), str(row[1]), str(row[2])) for row in result]
print(labels_list)


def create_rdf_graph(df):
    graph = rdflib.Graph()
    namespace_data_graph = URIRef("http://www.slb.com/data-graphs/Troubleshooting_ORA_FNFM_Data_graph#")
    namespace_ontology = URIRef("http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#")


    for index, row in df.iterrows():
        # Créer une URI pour chaque valeur dans les colonnes
        col1_uri = URIRef(namespace_data_graph + str(row.iloc[0]).replace(" ", "_"))
        col2_uri = URIRef(namespace_data_graph + str(row.iloc[1]).replace(" ", "_"))
        col3_uri = URIRef(namespace_data_graph + str(row.iloc[2]).replace(" ", "_"))
        col4_uri = URIRef(namespace_data_graph + str(row.iloc[3]).replace(" ", "_"))
        col5_uri = URIRef(namespace_data_graph + str(row.iloc[4]).replace(" ", "_"))
        col6_uri = URIRef(namespace_data_graph + str(row.iloc[5]).replace(" ", "_"))


        # Ajouter les valeurs comme des ressources dans le graphe
        graph.add((col1_uri, RDF.type, URIRef(namespace_ontology + "Failure")))
        graph.add((col2_uri, RDF.type, URIRef(namespace_ontology + "Failure")))
        graph.add((col3_uri, RDF.type, URIRef(namespace_ontology + "RootCause")))
        graph.add((col4_uri, RDF.type, URIRef(namespace_ontology + "RootCause")))
        graph.add((col5_uri, RDF.type, URIRef(namespace_ontology + "Trigger")))
        graph.add((col6_uri, RDF.type, URIRef(namespace_ontology + "DataChannel")))

        graph.add((col1_uri, RDFS.label, Literal(str(row.iloc[0]))))
        graph.add((col2_uri, RDFS.label, Literal(str(row.iloc[1]))))
        graph.add((col3_uri, RDFS.label, Literal(str(row.iloc[2]))))
        graph.add((col4_uri, RDFS.label, Literal(str(row.iloc[3]))))
        graph.add((col5_uri, RDFS.label, Literal(str(row.iloc[4]))))
        graph.add((col6_uri, RDFS.label, Literal(str(row.iloc[5]))))

        # Lier la première colonne à la deuxième avec une relation spécifique
        graph.add((col1_uri, URIRef(namespace_ontology + "cause"), col2_uri))
        graph.add((col1_uri, URIRef(namespace_ontology + "hasRootCause"), col3_uri))
        graph.add((col3_uri, URIRef(namespace_ontology + "next"), col4_uri))
        graph.add((col3_uri, URIRef(namespace_ontology + "isTriggeredBy"), col5_uri))
        graph.add((col5_uri, URIRef(namespace_ontology + "consume"), col6_uri))

    return graph

g = create_rdf_graph(df)
output_path = "output_ORA_FNFM_KG.ttl"
g.serialize(output_path, format="turtle")
















