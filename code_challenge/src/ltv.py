"""
created by Alena Kragel
"""
import os
import ast
import datetime
import re
import string
# main data structure dictionry 
D = {'CUSTOMER':{},'SITE_VISIT':{},'IMAGE':{},'ORDER':{}}
# list of indexes for main data tructure D
D_list_idx = {'event_time':0,'last_name':1,'adr_city':2,'adr_state':3,'customer_id':1,'tags':2,'camera_make':2,'camera_model':3,'total_amount':2}
# list of sizes to add in Customer_id key and list as its values
D_list_size = {'CUSTOMER':5,'SITE_VISIT':4,'IMAGE':5,'ORDER':4}

# check validation for given input tables names
valid_event_types = ('CUSTOMER','SITE_VISIT','IMAGE','ORDER')

# secondary data structure -  list of indexes for d - dictionary
d_list_idx = {'total_site_visits_counter':0,'sum_amount_orders':1}

def Input(path_to_files):
    """
    Load all events from file into local data structure
    :param path_to_files: given path to files location
    """
    extension = '.txt'
    txt = [i for i in os.listdir(path_to_files) if os.path.splitext(i)[1] == extension]
    for file in txt:
        file_path = os.path.join(path_to_files,file)
        try:
            with open(file_path) as file_object:
                for line in file_object:
                    if not line.isspace():
                        Ingest(ast.literal_eval(line.strip('\n\r[],')),D)

        except EOFError:
            print('No access to file: ', file_path)

def Ingest(event,D):
    """
    Insert or update given data with given event
    :param event: line from file (string)
    :param D: global data structure (dictionary)
    :return:
    """
    event_type_name = event['type']
    verb_name = event['verb']
    key_value = event['key']

    # Skip undefined event_type or empty key, verb
    if event_type_name not in valid_event_types or not verb_name or not key_value:
        return
    
    # Check given verb request for new event record
    if verb_name == 'NEW' or verb_name == 'UPLOAD':
        D[event_type_name][key_value] = [None]*D_list_size[event_type_name]

    # Update main D with event data iinstead of check for UPDATE
    for k,v in event.items():
        if k in D_list_idx.keys():
            list_field_ids = D_list_idx[k]
            D[event_type_name][key_value][list_field_ids] = v

def ConvertDate(date_event):
    """
    Find week number and year of given event
    :param date_event: given event date (string)
    :return: number week and year (int)
    """
    trans_dt = re.search(r'(\d+-\d+-\d+)', date_event).group(1)
    date_of_event = datetime.datetime.strptime(trans_dt,'%Y-%m-%d').date()
    week_of_event = date_of_event.isocalendar()[1]
    year_of_event = date_of_event.isocalendar()[0]

    return week_of_event, year_of_event

def ZipDate(dict_database, customer_id_idx, d, idx, sum_strategy):
    """
    Zip data by year and number of week
    :param dict_database: given dictionary (site_visits or orders)
    :param customer_id_idx: index of customer from d_list_idx (int)
    :param d: secondary data storage dictionary
    :param idx: index of list where to store given param_value (int)
    :param sum_strategy: logical switch between site visits and orders (string)
    """
    for key, values in dict_database.items():
        customer_id = values[customer_id_idx]
        if sum_strategy == 'orders':
            param_value = float(values[D_list_idx['total_amount']].strip(string.ascii_letters).replace(' ', ''))
        else:
            param_value = 1
        week_of_event, year_of_event = ConvertDate(values[D_list_idx['event_time']])

        if year_of_event not in d.keys():
            d[year_of_event]={}
        if week_of_event not in d[year_of_event].keys():
            d[year_of_event][week_of_event]={}
        if customer_id not in d[year_of_event][week_of_event].keys():
            # [0]* size of d_list_idx creates empty list given size in dictionary
            d[year_of_event][week_of_event][customer_id] = [0]* len(d_list_idx)

        d[year_of_event][week_of_event][customer_id][idx] += param_value

def Output(customers,x):
    """
    Write given output to file
    :param customers: customer id and their ltv (dictionary)
    :param x: top number record written in file (int)
    """
    file_name = 'output.txt'
    location = os.path.join(os.getcwd(),'output/')
    file_path = os.path.join(location,file_name)

    with open(file_path, 'w') as file:
        for customer_id, ltv in sorted(customers.items(), key = lambda c: c[1], reverse = True)[:x]:
            file.write(customer_id +' : ' + str(ltv)+ ', ')

def TopXSimpleLTVCustomers(x, D):
    """
    Write into output file top x customers with the highest customer livetime calue
    :param x: top number record written in file
    :param D: data for analysis
    """
    # secondary data structure
    d = {}

    customer_id_idx = D_list_idx['customer_id']
    ZipDate(D['SITE_VISIT'],customer_id_idx,d,d_list_idx['total_site_visits_counter'], 'visits')
    ZipDate(D['ORDER'],customer_id_idx,d,d_list_idx['sum_amount_orders'],'orders')

    years = {}
    for year, values1 in d.items():
        for week_number, values2 in values1.items():
            for customer_id_value, values3 in values2.items():
                customer_value_weekly = values3[d_list_idx['total_site_visits_counter']]\
                                                 * values3[d_list_idx['sum_amount_orders']]
                if year not in years.keys():
                   years[year]={}
                if customer_id_value in years[year].keys():
                    years[year][customer_id_value] += customer_value_weekly
                else:
                    years[year][customer_id_value] = customer_value_weekly

    avg_company_lifespan = 10
    customers = {}
    for year, values1 in years.items():
        for customer_id, customer_value in values1.items():
            var_ltv = round(52 * customer_value * avg_company_lifespan, 2)
            customers[customer_id] = var_ltv
    Output(customers,x)


def Main():
    
    join_path = '../'
    path_to_files = os.path.join(os.path.dirname(__file__) ,join_path)
    os.chdir(path_to_files)
    Input(path_to_files)
    TopXSimpleLTVCustomers(10, D)
 

if __name__ == '__main__':
    Main()