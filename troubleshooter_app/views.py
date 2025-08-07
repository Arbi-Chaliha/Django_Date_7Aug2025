import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef
from rdflib.namespace import OWL, RDF, RDFS, FOAF, XSD, DC, SKOS
from pyvis.network import Network
import duckdb
import os
import tempfile
import shutil # For moving the graph file
from sqlalchemy import create_engine, text
from django.shortcuts import render
from django.conf import settings
from django.http import HttpResponse
from .forms import TroubleshooterForm
import urllib.parse
from dotenv import load_dotenv

# --- 1. Load turtle file (Global for the app, loaded once) ---
# Ensure the path is correct relative to BASE_DIR
file_path = os.path.join(settings.BASE_DIR, 'data', 'output_ORA_FNFM_KG.ttl')
g = Graph()
try:
    g.parse(file_path, format='turtle')
    print("Ontology loaded successfully.")
except Exception as e:
    print(f"Error loading ontology: {e}")
    # Handle error appropriately, e.g., show a message to the user

# --- Teradata Connection (Global for the app, or managed per request if preferred) ---
load_dotenv()
user = os.getenv("TERADATA_USER")
pasw = os.getenv("TERADATA_PASS")
host = os.getenv("TERADATA_HOST")
port = os.getenv("TERADATA_PORT")

encoded_pass = urllib.parse.quote_plus(pasw)

# Create the engine
td_engine = None
try:
    td_engine = create_engine(
        f'teradatasql://{user}:{encoded_pass}@{host}/?encryptdata=true'
    )
    print("Teradata engine created successfully.")
except Exception as e:
    print(f"Error creating Teradata engine: {e}")
    # Handle error appropriately

# --- 2. Functions creation for triples extractions ---

def execute_query_for_concept(concept):
    query = f"""
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
    PREFIX troubleshooting_ora_fnfm_data_graph: <http://www.slb.com/data-graphs/Troubleshooting_ORA_FNFM_Data_graph#>
    PREFIX troubleshooting_ora_fnfm_ontology_: <http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    SELECT DISTINCT ?subject_label (STRAFTER(STR(?predicate), "#") AS ?predicateName) ?object_label
    WHERE {{
        ?subject_uri ?predicate ?object_uri;
                rdfs:label "{concept}";
                rdfs:label ?subject_label.
        ?subject_uri a ?type_subject.
        ?object_uri a ?type_object;
                rdfs:label ?object_label.
        FILTER (?type_subject IN (troubleshooting_ora_fnfm_ontology_:Failure, troubleshooting_ora_fnfm_ontology_:RootCause, troubleshooting_ora_fnfm_ontology_:Trigger, troubleshooting_ora_fnfm_ontology_:DataChannel) || 
            ?type_object IN (troubleshooting_ora_fnfm_ontology_:Failure, troubleshooting_ora_fnfm_ontology_:RootCause, troubleshooting_ora_fnfm_ontology_:Trigger, troubleshooting_ora_fnfm_ontology_:DataChannel))
        FILTER (?predicate != rdf:type)
    }}
    """
    result = g.query(query)
    labels_list = [(str(row[0]), str(row[1]), str(row[2])) for row in result]
    return labels_list


def graph_search_tuple(concept, visited=None, result=None, max_depth=-1, depth=0, depth_results=None):
    """
    Return a dictionary of lists of triples for each depth for a specified concept.
    """
    if visited is None:
        visited = []
    if result is None:
        result = []
    if depth_results is None:
        depth_results = {}

    if max_depth != -1 and depth >= max_depth:
        return depth_results

    if concept in visited:
        return depth_results

    visited.append(concept)

    results_query = execute_query_for_concept(concept)

    if depth not in depth_results:
        depth_results[depth] = []
    depth_results[depth].extend([elem for elem in results_query if elem not in depth_results[depth]])

    result.extend([elem for elem in results_query if elem not in result])

    for concept1, message, concept2 in results_query:
        graph_search_tuple(concept2, visited, result, max_depth, depth + 1, depth_results)

    return depth_results

# --- 3. Teradata Query Functions ---
# These functions will now take a connection object (conn) as an argument

def threshold_sup_10450(conn, partition_id, triple_subject):
    sql = f""" sel sum(error_count) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_LIMIT_CHECK_PER_JOB
    where xcol = 'MCDIGVLTFM' and (metric_name = 'above_sigma_one'
    or metric_name = 'below_sigma_one') and partition_id = {partition_id}"""
    df = pd.read_sql(sql, conn)
    result_value = df.iloc[0, 0]
    return result_value > 10450

def threshold_sup_12000(conn, partition_id, triple_subject):
    sql = f""" sel sum(error_count) as sum_error_count
    from PRD_RP_PRODUCT_VIEW.FNFM_LIMIT_CHECK_PER_JOB
    where xcol = 'MCREFVLTFM' and partition_id = {partition_id} """
    df = pd.read_sql(sql, conn)
    result_value = df.iloc[0, 0]
    return result_value is not None and result_value > 12000

def threshold_sup_5000(conn, partition_id, triple_subject):
    sql = f""" sel sum(error_count) as sum_error_count
    from PRD_RP_PRODUCT_VIEW.FNFM_LIMIT_CHECK_PER_JOB
    where (metric_name = 'above_sigma_one' or metric_name = 'below_sigma_one') and xcol = 'MCINVLTFM' and partition_id = {partition_id} """
    df = pd.read_sql(sql, conn)
    result_value = df.iloc[0, 0]
    return result_value > 5000

def discrete_sup_10(conn, partition_id, triple_subject):
    sql = f""" sel sum(count_error) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_STATUS_WORDS_AGGREGATED_PER_JOB
    where xcol = '{triple_subject}' and xcol_decoded = 'FNFM_TripPhaseAFM' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, conn)
    result_value = df.iloc[0, 0]
    return int(result_value) > 10 if result_value is not None else False

def discrete_sup_20(conn, partition_id, triple_subject):
    sql = f""" sel sum(count_error) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_STATUS_WORDS_AGGREGATED_PER_JOB
    where xcol = '{triple_subject}' and xcol_decoded = 'FNFM_EIPUplinkMessageSend' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, conn)
    result_value = df.iloc[0, 0]
    return int(result_value) > 20 if result_value is not None else False

def mcrterrfm_check(conn, partition_id, triple_subject):
    sql = f""" sel sum(count_error) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_STATUS_WORDS_AGGREGATED_PER_JOB
    where xcol = 'MCRTERRFM' and xcol_decoded in ('FNFM_EIPUplinkMessageSend','FNFM_EIPITCMessageSend', 'FNFM_EIPLoopbackMessageSend', 'FNFM_EIPDownlinkMessageReceive') and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, conn)
    result_value = df.iloc[0, 0]
    return int(result_value) > 1 if result_value is not None else False

def limit_check(conn, partition_id, triple_subject):
    sql = f""" sel sum(error_count),min("min"),max("max")
    from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_generic_limit_checks_agg_mavg
    where xcol = '{triple_subject}' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, conn)
    result_value = df.iloc[0, 0]
    return int(result_value) > 0 if result_value is not None else False

def status_check(conn, partition_id, triple_subject):
    sql = f""" sel partition_id
    from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_generic_status_checks
    where event_name = '{triple_subject}' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, conn)
    return not df.empty

def large_pump(conn, partition_id, triple_subject):
    sql = f""" sel partition_id
    from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_large_pump_cal_check
    where health_indicator = 'Fail' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, conn)
    return not df.empty

def small_pump(conn, partition_id, triple_subject):
    sql = f""" sel partition_id
    from PRD_GLBL_DATA_PRODUCTS.FNFM_fleet_timeseries_small_pump_cal_check
    where health_indicator = 'Fail' and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, conn)
    return not df.empty

def mterrstafm_check(conn, partition_id, triple_subject):
    sql = f""" sel sum(count_error) as count_of_error
    from PRD_RP_PRODUCT_VIEW.FNFM_STATUS_WORDS_AGGREGATED_PER_JOB
    where xcol = 'MTERRSTAFM' and xcol_decoded in ('FNFM_FaultIbusFM', 'FNFM_TripPhaseBFM', 'FNFM_TripPhaseCFM', 'FNFM_FaultIbFM', 'FNFM_FaultIaFM', 'FNFM_TripPhaseAFM') and partition_id= '{partition_id}' """
    df = pd.read_sql(sql, conn)
    result_value = df.iloc[0, 0]
    return int(result_value) > 1 if result_value is not None else False

# --- 4. Mapping condition and function ---
def execute_function_from_the_map(message, mapping, conn, partition_id, datachannel):
    """
    Execution of the function
    """
    if message in mapping:
        return mapping[message](conn, partition_id, datachannel) # Pass conn here

def recursive_execute_function(dict_tuple_result, mapping, conn, partition_id):
    """
    Recursive execution of all functions
    """
    result_list = []
    all_tuples = [t for tuples in dict_tuple_result.values() for t in tuples]
    df_tuples = pd.DataFrame(all_tuples, columns=['Subject', 'Predicate', 'Object'])
    query_trigger_datachannel = """
    SELECT DISTINCT t1.Object AS Trigger,t2.Predicate AS Consume, t2.Object AS DataChannel
    FROM df_tuples t1
    JOIN df_tuples t2 ON t1.Object = t2.Subject
    WHERE t1.Predicate = 'isTriggeredBy' AND t2.Predicate = 'consume'
    """
    result_df = duckdb.query(query_trigger_datachannel).to_df()
    for index, row in result_df.iterrows():
        function = row.iloc[0]
        consume = row.iloc[1]
        datachannel = row.iloc[2]
        result = execute_function_from_the_map(function, mapping, conn, partition_id, datachannel)
        result_list.append((function, consume, datachannel, result))
    df = pd.DataFrame(result_list, columns=['Subject', 'Predicate', 'Object', 'Status'])
    return df

# --- Main Django View ---
def troubleshooter_view(request):
    form = TroubleshooterForm()
    failure_list = []
    df_metadata = pd.DataFrame()
    serial_number_choices = []
    job_number_choices = []
    job_start_choices = []
    partition_id = None
    df_clean = pd.DataFrame()
    root_cause_table_data = []
    graph_html_path = None
    messages = [] # To store messages like errors or successful operations

    # Ensure Teradata connection is available
    if td_engine is None:
        messages.append("Error: Could not connect to Teradata. Please check credentials and connection settings.")
        context = {
            'form': form,
            'messages': messages,
            'failure_list': failure_list,
            'partition_id': partition_id,
            'df_clean_html': None,
            'root_cause_table_html': None,
            'graph_html_path': graph_html_path,
        }
        return render(request, 'troubleshooter.html', context)

    try:
        with td_engine.connect() as conn:
            # Populate initial failure list for the selectbox
            query= """
PREFIX troubleshooting_ora_fnfm_ontology_: <http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT ?failure
WHERE {
  ?failure_uri a troubleshooting_ora_fnfm_ontology_:Failure ;
  rdfs:label ?failure
}
"""
            failure_query_result = g.query(query)
            df_failures = pd.DataFrame(failure_query_result, columns=["failure"])
            failure_list = df_failures["failure"].tolist()

            # Populate initial serial number choices
            sql_metadata = """sel * from PRD_RP_PRODUCT_VIEW.FNFM_FLEET_METADATA"""
            df_metadata = pd.read_sql(sql_metadata, conn)
            serial_number_choices = sorted([(str(x), str(x)) for x in df_metadata["serial_number"].fillna('NaN').unique()])
            form.fields['serial_number'].choices = [('', 'Select serial number...')] + serial_number_choices

            if request.method == 'POST':
                form = TroubleshooterForm(request.POST)
                # Re-populate choices for the form if it's a POST request
                # This ensures that if the user changes serial number, job number choices update
                form.fields['serial_number'].choices = [('', 'Select serial number...')] + serial_number_choices

                selected_serial_number = request.POST.get('serial_number')
                selected_job_number = request.POST.get('job_number')
                selected_job_start = request.POST.get('job_start')
                selected_failure = request.POST.get('failure_selectbox') # This will come from the template's hidden input or direct select

                # Dynamic population of job_number and job_start based on selections
                if selected_serial_number and selected_serial_number != 'NaN':
                    df_serial_number = df_metadata[df_metadata["serial_number"] == selected_serial_number]
                    job_number_choices = sorted([(str(x), str(x)) for x in df_serial_number["job_number"].fillna('NaN').unique()])
                    form.fields['job_number'].choices = [('', 'Select job number...')] + job_number_choices

                    if selected_job_number and selected_job_number != 'NaN':
                        df_serial_and_job_number = df_serial_number[df_serial_number["job_number"] == selected_job_number]
                        job_start_choices = sorted([(str(x), str(x)) for x in df_serial_and_job_number["job_start"].fillna('NaN').unique()])
                        form.fields['job_start'].choices = [('', 'Select start job...')] + job_start_choices

                # Process if all required fields are selected
                if selected_serial_number and selected_job_number and selected_job_start and selected_failure:
                    try:
                        sql_partition_id = f"""
                        sel partition_id
                        from PRD_RP_PRODUCT_VIEW.FNFM_FLEET_METADATA
                        where
                        serial_number = '{selected_serial_number}' and
                        job_number = '{selected_job_number}' and
                        CAST(job_start AS CHAR(26)) = '{selected_job_start}';"""

                        df_partition_id = pd.read_sql(sql_partition_id, conn)
                        if not df_partition_id.empty:
                            partition_id = df_partition_id.iloc[0, 0]
                            messages.append(f"The partition_id associated with your chosen serial number, job number and start job is {partition_id}")

                            # --- Execute the core logic ---
                            dic_tuple_result = graph_search_tuple(selected_failure, max_depth=-1)

                            mapping_function = {
                                "FNFM Uplink telemetry check": status_check,
                                "FNFM LIN device check": status_check,
                                "FNFM CAN device check": status_check,
                                "FNFM Motor Error Status": mterrstafm_check,
                                "FNFM Solenoid PHM HALL Voltage": limit_check,
                                "FNFM Solenoid PHM Digital Voltage": limit_check,
                                "FNFM Solenoid PHM LIN Voltage ADC": limit_check,
                                "FNFM Master Controller Reference Voltage": limit_check,
                                "FNFM Master Controller Digital Voltage": limit_check,
                                "FNFM Master Controller Input Voltage": limit_check,
                                "FNFM Master Controller Core Voltage": limit_check,
                                "FNFM Master Controller EIP Core Voltage": limit_check,
                                "FNFM Master Controller EIP Digital Voltage": limit_check,
                                "FNFM LVPS Digital Voltage": limit_check,
                                "FNFM LVPS Positive Analog Voltage": limit_check,
                                "FNFM LVPS Negative Analog Voltage": limit_check,
                                "FNFM Small pump calibration check": small_pump,
                                "FNFM Large pump calibration check": large_pump
                            }

                            result_df_functions = recursive_execute_function(dic_tuple_result, mapping_function, conn, partition_id)

                            all_tuples = [t for tuples in dic_tuple_result.values() for t in tuples]
                            df_tuples = pd.DataFrame(all_tuples, columns=['Subject', 'Predicate', 'Object'])
                            df_final = pd.merge(df_tuples, result_df_functions, on=["Subject", "Predicate", "Object"], how="left")
                            df_clean = df_final[df_final["Status"].apply(lambda x: x is not None)]

                            # --- Root Cause Analysis Table ---
                            query_rootcause = f"""
                                SELECT Object
                                FROM df_clean
                                WHERE Subject = '{selected_failure}' AND Predicate = 'hasRootCause'
                            """
                            try:
                                rootcause_df = duckdb.query(query_rootcause).to_df()
                                if not rootcause_df.empty:
                                    for root_cause in rootcause_df["Object"]:
                                        query_trigger = f"""
                                            SELECT Object
                                            FROM df_clean
                                            WHERE Subject = '{root_cause}' AND Predicate = 'isTriggeredBy'
                                        """
                                        trigger_df = duckdb.query(query_trigger).to_df()

                                        if not trigger_df.empty:
                                            for trigger_value in trigger_df["Object"]:
                                                query_datachannel = f"""
                                                    SELECT DISTINCT Object, Status
                                                    FROM df_clean
                                                    WHERE Subject='{trigger_value}' AND Predicate='consume' AND Status=True
                                                """
                                                datachannel_df = duckdb.query(query_datachannel).to_df()

                                                if not datachannel_df.empty:
                                                    for _, row in datachannel_df.iterrows():
                                                        symbol = "🔴"
                                                        root_cause_table_data.append([root_cause, trigger_value, f"{row['Object']} {symbol}"])
                                else:
                                    messages.append("No root causes found for the selected failure.")
                            except Exception as e:
                                messages.append(f"Error during root cause analysis: {e}")

                            # --- Pyvis Graph Generation ---
                            net = Network(height="1100px", width="100%", directed=True, notebook=True) # notebook=True for standalone HTML

                            for _, row in df_clean.iterrows():
                                subject = row['Subject']
                                predicate = row['Predicate']
                                object_node = row['Object']
                                status = row['Status']

                                # Define colors and titles based on predicate and status
                                color_subject = "#A7C7E7" # Default
                                color_object = "#A7C7E7" # Default
                                color_predicate = "#A7C7E7" # Default
                                title_subject = f"name:{subject}"
                                title_object = f"name:{object_node}"
                                title_predicate = f"name:{predicate}"

                                if predicate == "hasRootCause":
                                    color_subject = "#FFCC99"
                                    color_object = "#C5A3FF"
                                    title_subject = f"type:failure, name:{subject}"
                                    title_object = f"type:Root Cause, name:{object_node}"
                                elif predicate == "isTriggeredBy":
                                    color_subject = "#C5A3FF"
                                    color_object = "#D2B48C"
                                    title_subject = f"type:Root Cause, name:{subject}"
                                    title_object = f"type:Trigger, name:{object_node}, value:{status}"
                                elif predicate == "next":
                                    color_subject = "#C5A3FF"
                                    color_object = "#C5A3FF"
                                    title_subject = f"type:Root Cause, name:{subject}"
                                    title_object = f"type:Root Cause, name:{object_node}"
                                elif predicate == "cause":
                                    color_subject = "#FFCC99"
                                    color_object = "#FFCC99"
                                    title_subject = f"type:Failure, name:{subject}"
                                    title_object = f"type:Failure, name:{object_node}"
                                elif predicate == "consume":
                                    color_subject = "#D2B48C"
                                    title_subject = f"type:trigger, name:{subject}"
                                    title_object = f"type:data channel, name:{object_node}"
                                    if status == False:
                                        color_object = "green"
                                        color_predicate = "green"
                                    elif status == True:
                                        color_object = "red"
                                        color_predicate = "red"

                                net.add_node(subject, color=color_subject, label=subject, title=title_subject)
                                net.add_node(object_node, color=color_object, label=object_node, title=title_object)
                                net.add_edge(subject, object_node, color=color_predicate, title=title_predicate)

                            net.force_atlas_2based(gravity=-50, central_gravity=0.01, spring_length=200, spring_strength=0.05)

                            # Save the graph to the static/graphs directory
                            graph_filename = f"graph_{partition_id}.html"
                            graph_output_path = os.path.join(settings.STATICFILES_DIRS[0], 'graphs', graph_filename)
                            net.save_graph(graph_output_path)
                            graph_html_path = os.path.join(settings.STATIC_URL, 'graphs', graph_filename) # URL to access it

                        else:
                            messages.append("Error: Could not find partition_id for the selected criteria.")

                    except Exception as e:
                        messages.append(f"An error occurred during data processing: {e}")
                else:
                    messages.append("Please select all fields (Serial Number, Job Number, Start Job, and Failure) to proceed.")

    except Exception as e:
        messages.append(f"An unexpected error occurred: {e}")

    # Prepare data for rendering
    df_clean_html = df_clean.to_html(classes='table table-striped table-bordered', index=False) if not df_clean.empty else None
    root_cause_table_html = pd.DataFrame(root_cause_table_data, columns=["Root Cause", "Trigger", "Data Channel"]).to_html(classes='table table-striped table-bordered', index=False) if root_cause_table_data else None


    context = {
        'form': form,
        'messages': messages,
        'failure_list': failure_list,
        'partition_id': partition_id,
        'df_clean_html': df_clean_html,
        'root_cause_table_html': root_cause_table_html,
        'graph_html_path': graph_html_path,
    }
    return render(request, 'troubleshooter.html', context)

# Create your views here.
