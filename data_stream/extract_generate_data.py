from utils import SinumerikTraceHandler
from tqdm import tqdm
from os import walk
import json
import time
import pandas as pd

import paho.mqtt.client as mqtt
import paho.mqtt.properties as mqttprops


DATA_DIR = "../data/"

ALL_TOPICS = set()
# mqtt

broker_address = "localhost"
port = 1883

# Function to connect to MQTT broker
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)

# # Initialize MQTT client
# client = mqtt.Client()
# client.on_connect = on_connect
# client.connect(broker_address, port, 60)
# client.loop_start()

# props = mqttprops.Properties(mqttprops.PacketTypes.PUBLISH)
# props.UserProperty = ("Content-Type", "application/json")


def get_df(xml_path):
    handler = SinumerikTraceHandler()
    return handler.read_xml(xml_path)


# ServoDataActCurr
# ServoDataContDev
# ServoDataActPos1st
# ServoDataActPos2
# diff = abs ServoDataActPos1st ServoDataActPos2

# sort by axes:

def get_axes_from_filename(file):
    elems = file.split('_')
    if 'X' in elems:
        return [1,'X']
    elif 'Y' in elems:
        return [2, 'Y']
    elif 'Z' in elems:
        return [3, 'Z']
    elif 'C' in elems:
        return [4, 'C']
    elif 'A' in elems:
        return [5, 'A']
    else:
        return None


def get_data_from_df(client, df, axes):
    print('topics till now')
    print(ALL_TOPICS)
    for index, row in tqdm(df.iterrows()):
        sess = row['sessionName'].split('_')
        # topic = '/'.join([sess[-1], '_'.join([sess[0], sess[1], sess[2], sess[3]])])+'/'
        topic = '/'.join([sess[3], sess[-1]])+'/'
        ALL_TOPICS.add(topic)
        # topic_base = row['meas_type_id']+'/'+sess+'/'
        msg = {}
        msg['ServoDataActCurr'] = row[f'/Nck/!SD/nckServoDataActCurr64 [u1, {axes}]']
        msg['ServoDataContDev'] = row[f'/Nck/!SD/nckServoDataContDev64 [u1, {axes}]']
        msg['ServoDataActPos1'] = row[f'/Nck/!SD/nckServoDataActPos1stEnc64 [u1, {axes}]']
        msg['ServoDataActPos2'] = row[f'/Nck/!SD/nckServoDataActPos2ndEnc64 [u1, {axes}]']
        msg['PosAbsDiff'] = abs(row[f'/Nck/!SD/nckServoDataActPos1stEnc64 [u1, {axes}]'] - row[f'/Nck/!SD/nckServoDataActPos2ndEnc64 [u1, {axes}]'])
        msg['StartTimeStamp'] = row['start_time']
        mqtt_message = json.dumps(msg)

        # print(mqtt_message)
        # print(topic)

        client.publish(topic, mqtt_message, qos=1, properties=props)
        
        # delay = 10
        # time.sleep(delay/1000)
        # set ms delay
        # delay = 100
        # timelibrary.sleep(delay/1000)
        # # motor current
        # mc_topic = topic_base + 'ServoDataActCurr'
        # cd_topic = topic_base + 'ServoDataContDev'
        # srv_p1_topic = topic_base + 'ServoDataActPos1'
        # srv_p2_topic = topic_base + 'ServoDataActPos2'
        # diff_srv_topic = topic_base + 'ServoDataDiff'

        # print(mc_topic)
        # print(cd_topic)
        # print(srv_p1_topic)
        # print(srv_p2_topic)
        # print(diff_srv_topic)
        # break

def load_xml_data_from_directory(directory):
    """
    Load and parse XML files from a specified directory.
    
    Parameters:
    - directory: Path to the directory containing XML files.
    
    Returns:
    - DataFrame containing the combined data from all XML files.
    """
    all_data = []
    df = pd.DataFrame(all_data)
    xml_files = glob.glob(os.path.join(directory, '*.xml'))
    # print(f'xml file: {xml_files}')
    # file_path = '240506_49514579_GL_Z_F2000.xml'
    # tree = ET.parse('./data/240506_49514579_GL_Z_F2000.xml')

    # print(f"Tree: {tree}")
    # root = tree.getroot()
    # print(f"Root: {root}")
        
    # for entry in root.iter('rec'):
    #     # print(f'Entry: {entry}')
    #     # data_dict = {
    #     #         'time': float(entry.get('time')),
    #     #         'f1': float(entry.get('f1')),
    #     #         'f2': float(entry.get('f2')),
    #     #         'f3': float(entry.get('f3')),
    #     #         'f4': float(entry.get('f4')),
    #     #         'f5': float(entry.get('f5'))
    #     #     }
    #     rec_data = {
    #             'time': entry.attrib.get('time'),
    #             'f1': entry.attrib.get('f1'),
    #             'f2': entry.attrib.get('f2'),
    #             'f3': entry.attrib.get('f3'),
    #             'f4': entry.attrib.get('f4'),
    #             'f5': entry.attrib.get('f5')
    #             }
    #     # print(f'Data point found: {rec_data}')
    #     all_data.append(rec_data)
    # # print(f'All data in tree: {all_data}')
    # df = pd.DataFrame(all_data)

    # # Convert the columns to the appropriate data types
    # df = df.astype({'time': 'float', 'f1': 'float', 'f2': 'float', 'f3': 'float', 'f4': 'float', 'f5': 'float'})

    # # print(pd.DataFrame(all_data))

    for file in xml_files:
        all_data = []
        tree = ET.parse(file)
        file_path = file
        file_path = file_path.replace('./data/', '')

        # print(f"Tree: {tree}")
        root = tree.getroot()
        # print(f"Root: {root}")

        filename = os.path.basename(file_path)
        print(f'Filename: {filename}')
        date, equipment_nr, gl, axis, velocity = filename.split('_')
        # axis, velocity = axis_velocity.split('.')

        # Remove '.xml' from the velocity
        velocity = velocity.replace('.xml', '')

        # Add the metadata as new columns to the DataFrame
        # df['date'] = date
        # df['equipment_nr'] = equipment_nr
        # # df['gl'] = gl
        # df['axis'] = axis
        # df['velocity'] = velocity
        
        for entry in root.iter('rec'):
            # print(f'Entry: {entry}')
            # data_dict = {
            #         'time': float(entry.get('time')),
            #         'f1': float(entry.get('f1')),
            #         'f2': float(entry.get('f2')),
            #         'f3': float(entry.get('f3')),
            #         'f4': float(entry.get('f4')),
            #         'f5': float(entry.get('f5'))
            #     }
            rec_data = {
                    'time': entry.attrib.get('time'),
                    'f1': entry.attrib.get('f1'),
                    'f2': entry.attrib.get('f2'),
                    'f3': entry.attrib.get('f3'),
                    'f4': entry.attrib.get('f4'),
                    'f5': entry.attrib.get('f5'),
                    'date': date,
                    'equipment_nr': equipment_nr,
                    'axis': axis,
                    'velocity': velocity
                    }
            # print(f'Data point found: {rec_data}')
            all_data.append(rec_data)
            
    #     # print(f'All data in tree: {all_data}')
    #     # df.append(all_data)
        all_data = pd.DataFrame(all_data)
        df = pd.concat([df, all_data])

        # Convert the columns to the appropriate data types
        # df = df.astype({'time': 'float', 'f1': 'float', 'f2': 'float', 'f3': 'float', 'f4': 'float', 'f5': 'float'})
        
        # print(df)
    
    return df


def main(client):

    # topics = {
    #     'Z/F2000/', 
    #     'Y/F4000/', 
    #     'C/F720/', 
    #     'X/F2000/', 
    #     'C/F1000/', 
    #     'A/F360/', 
    #     'Z/F6000/', 
    #     'C/F360/', 
    #     'X/F6000/', 
    #     'Y/F6000/', 
    #     'Z/F4000/', 
    #     'A/F420/', 
    #     'Y/F2000/', 
    #     'X/4000/', 
    #     'X/F4000/', 
    #     'A/F300/'
    #     }



    filenames = next(walk(DATA_DIR), (None, None, []))[2]
    maintenance_data = load_xml_data_from_directory(DATA_DIR)
    x_list = []
    y_list = []
    z_list = []
    c_list = []
    a_list = []

    print('preparing data by axes...')
    
    for file in tqdm(filenames):
        axes = get_axes_from_filename(file)
        if axes[0] == 1:
            x_list.append(get_df(DATA_DIR+file))
        if axes[0] == 2:
            y_list.append(get_df(DATA_DIR+file))
        if axes[0] == 3:
            z_list.append(get_df(DATA_DIR+file))
        if axes[0] == 4:
            c_list.append(get_df(DATA_DIR+file))
        if axes[0] == 5:
            a_list.append(get_df(DATA_DIR+file))
        else:
            pass
    
    print('sorted data files by axes. merging into separate dataframes.')

    x_df = pd.concat(x_list)
    y_df = pd.concat(y_list)
    z_df = pd.concat(z_list)
    c_df = pd.concat(c_list)
    a_df = pd.concat(a_list)

    print('\nconcatenated dfs')

    print('\nprocessing X axis data')
    get_data_from_df(client, x_df, 1)

    print('\nprocessing Y axis data')    
    get_data_from_df(client, y_df, 2)

    print('\nprocessing Z axis data')
    get_data_from_df(client, z_df, 3)

    print('\nprocessing C axis data')    
    get_data_from_df(client, c_df, 4)

    print('\nprocessing A axis data')
    get_data_from_df(client, a_df, 5)

    print('\nALL TOPICS:\n')
    print(ALL_TOPICS)

    # Data Machine Learning and feature engineering pipeline
    for col in maintenance_data:
        print(maintenance_data[col].unique())

    print(maintenance_data['axis'].value_counts())
    print(maintenance_data['velocity'].value_counts())
    print(maintenance_data['equipment_nr'].value_counts())
    er = maintenance_data['equipment_nr'].value_counts()
    

    # Thereafter observations are:
    # Unique axis are X, Y, Z, A, C
    # Unique velocity are ['F2000' 'F1000' 'F360' 'F6000' 'F4000' 'F720' 'F300' 'F420']
    # Unique equipment_nr are ['49514576' '49514571' '49514580' '49514579' '49515264' '49514136' '49514569']

    # F1000 has many recordings as rec s
    # Unique counts are X:50, Z:48, C:48, Y:48, A:47
    # Unique counts are F6000:49, F4000:49, F2000:48, F360:32, F1000:16, F720:16, F420:16, F300:15
    print(maintenance_data.groupby(['equipment_nr','velocity','axis']).size().reset_index().rename(columns={0:'count'}))

    # All combinations / groups of equipment_nr, velocity and axis are in pair (total count of 2).

    print(maintenance_data.groupby(['equipment_nr','velocity']).size().reset_index())

    # Visualize the distributions of the features
    feature_cols = ['f1', 'f2', 'f3', 'f4', 'f5']

    # Filter out rows with None values
    filtered_data = maintenance_data.dropna()

    for feature in feature_cols:
        plt.figure(figsize=(10, 6))
        sns.histplot(filtered_data[feature], bins=30, kde=True)
        plt.title(f'Distribution of {feature}')
        plt.xlabel(feature)
        plt.ylabel('Frequency')
        plt.show()
        break
    
    # Plot the filtered data
    plt.plot(filtered_data["f1"])
    plt.show()

    plt.plot(filtered_data["f2"])
    plt.show()

    plt.plot(filtered_data["f3"])
    plt.show()

    plt.plot(filtered_data["f4"])
    plt.show()

    plt.plot(filtered_data["f5"])
    plt.show()
    
    #  So comments:
    # In general can be seen that f2,f3,f5 have incrementive and normal behaviour
    # Behaviour in f1 is really chaotic with increasing chaos and increasing upper bound
    # Behaviour in f4 is less chaotic than f1 and has a sudden jump in middle of the time.

    # Printing the variance as one of the feature engineering step
    print(filtered_data.var())

    # The other mathematical modelling steps are:
    from sklearn import cluster


    feature_cols = ['time', 'f1', 'f2', 'f3', 'f4', 'f5']
    # Dimensionality reduction using PCA
    pca = PCA(n_components=2) # arbitrary 2 dimensions chosen for visualization
    pca_result = pca.fit_transform(filtered_data[feature_cols])

    # Dimensionality reduction using UMAP
    reducer = umap.UMAP()
    umap_result = reducer.fit_transform(filtered_data[feature_cols])

    # Clustering using HDBSCAN
    clusterer = cluster.HDBSCAN()
    cluster_labels = clusterer.fit_predict(umap_result)

    # Add PCA, UMAP, and cluster labels to the dataframe
    filtered_data['pca_one'] = pca_result[:, 0]
    filtered_data['pca_two'] = pca_result[:, 1]
    filtered_data['umap_one'] = umap_result[:, 0]
    filtered_data['umap_two'] = umap_result[:, 1]
    filtered_data['cluster'] = cluster_labels

    # Filter the data based on the cluster number
    filtered_data = filtered_data[filtered_data['cluster'].notnull()]

    # Create a scatter plot
    plt.scatter(filtered_data['pca_one'], filtered_data['pca_two'], c=filtered_data['cluster'], cmap='viridis')
    plt.xlabel('PCA One')
    plt.ylabel('PCA Two')
    plt.title('PCA Visualization with respect to Cluster Number')
    plt.colorbar(label='Cluster Number')
    plt.show()


    # Visualize PCA results
    # plt.figure(figsize=(12, 8))
    # sns.scatterplot(x='pca_one', y='pca_two', hue='cluster', palette='viridis', data=maintenance_data)
    # plt.title('PCA Clustering Results')
    # plt.show()

    # Visualize UMAP results
    plt.figure(figsize=(12, 8))
    sns.scatterplot(x='umap_one', y='umap_two', hue='cluster', palette='viridis', data=filtered_data)
    plt.title('UMAP Clustering Results')
    plt.show()
# if __name__=="__main__":
    # Initialize MQTT client
print('starting data thrower client...')
client = mqtt.Client()
client.on_connect = on_connect
client.connect(broker_address, port, 60)
client.loop_start()

props = mqttprops.Properties(mqttprops.PacketTypes.PUBLISH)
props.UserProperty = ("Content-Type", "application/json")

main(client)

client.loop_stop()
client.disconnect()


# df_list = []
# for file in tqdm(filenames):
#     df_list.append(get_df(file))



# mqtt

# # for index, row in sorted_df.iterrows():
# for index, row in islice(sorted_df.iterrows(), START_ROW, END_ROW):
#     if index % 1000 == 0:
#         msg = {}
#         msg['param_id'] = row['param_id']
#         msg['value'] = row['Value']
#         # msg['server_time'] = row['Server_Time']
#         msg['server_time'] = convert_unix_ts(row['Server_Time'])
#         #message = f"Value: {row['Value']}, Server Time: {row['Server_Time']}"
#         #message = f"""{
#         #    	thing_id: {thing_id}, 
#         #        value: {row['Value']}, 
#         #        server_time: {row['Server_Time']}
#         #        }"""
#         #print(msg)
#         # print("value: ", row['Value'])
#         message = json.dumps(msg)

#         mqtt_topic = 'params/' + row['param_id']

#         client.publish(mqtt_topic, message, qos=1, properties=props)

#         # set ms delay
        # delay = 100
        # timelibrary.sleep(delay/1000)
        #client.publish(mqtt_topic, message, qos=1)


