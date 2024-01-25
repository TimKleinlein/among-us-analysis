import pandas as pd
import os
import ast
import pickle

df = pd.read_excel('Results.xlsx', sheet_name='SubstituteNones')
df['Old'] = df['Old'].apply(lambda lobbies: [int(x) if x.lower() != 'none' else None for x in lobbies.split(',')])
df['New'][7] = '2'  # was an integer before
df['New'] = df['New'].apply(lambda lobbies: [int(x) if x.lower() != 'none' else None for x in lobbies.split(',')])


sessions = os.listdir('../../data/initial_synchronization_output/assignedLobbiesDfs')

dataframes = {}
for session in sessions:
    dataframes[f'data_{session}'] = pd.read_csv(f'assignedLobbiesDfs/{session}')
for k, v in dataframes.items():
    dataframes[k]['lobbies_assigned_final'] = v['lobbies_assigned_final'].apply(lambda x: ast.literal_eval(x))


# substitute Nones with at beginning & end with correct lobbies
def substitute_nones(row):
    if row['path'] in list(df['Streamer']):
        old_values = list(df[df['Streamer'] == row['path']]['Old'])[0]
        new_values = list(df[df['Streamer'] == row['path']]['New'])[0]

        for i in range(len(row['lobbies_assigned_final']) - (len(old_values)-1)):
            if row['lobbies_assigned_final'][i:i + len(old_values)] == old_values:
                row['lobbies_assigned_final'][i:i + len(new_values)] = new_values


for k in dataframes.keys():
    data = dataframes[k]
    data.apply(lambda row: substitute_nones(row), axis=1)
    dataframes[k] = data


# kick remaining Nones
def kick_none(row):
    return [x for x in row['lobbies_assigned_final'] if x is not None]


for k in dataframes.keys():
    data = dataframes[k]
    data['lobbies_assigned_final'] = data.apply(lambda row: kick_none(row), axis=1)
    dataframes[k] = data


# remove doubled lobby assignments (f.e. 13,14,14,15)
def remove_doubles(row):
    unique_list = []
    seen = set()
    for lob in row['lobbies_assigned_final']:
        if lob not in seen:
            seen.add(lob)
            unique_list.append(lob)
    return unique_list


for k in dataframes.keys():
    data = dataframes[k]
    data['lobbies_assigned_final'] = data.apply(lambda row: remove_doubles(row), axis=1)
    dataframes[k] = data


# add missing lobbies in between minimum and maximum lobby
def add_missing_lobbies(row):
    new_list = [row['lobbies_assigned_final'][0]]
    for i in range(1, len(row['lobbies_assigned_final'])):
        if row['lobbies_assigned_final'][i] - row['lobbies_assigned_final'][i-1] != 1:
            diff = row['lobbies_assigned_final'][i] - row['lobbies_assigned_final'][i-1]
            for j in range(diff):
                new_list.append(row['lobbies_assigned_final'][i-1] + (j+1))
        else:
            new_list.append(row['lobbies_assigned_final'][i])
    return new_list


for k in dataframes.keys():
    data = dataframes[k]
    data['lobbies_assigned_final'] = data.apply(lambda row: add_missing_lobbies(row), axis=1)
    dataframes[k] = data


# correct manually the assignments for the two streamers which skipped a lobby
def skip_lobby(row, num):
    print(row)
    old_list = row['lobbies_assigned_final']
    new_list = [x for x in old_list if x != num]
    return new_list


data = dataframes['data_2022-01-26_S1.csv']
data.loc[data['path'] == '2022-01-26_S1_jvckk_1276935029', 'lobbies_assigned_final'] \
    = data[data['path'] == '2022-01-26_S1_jvckk_1276935029'].apply(lambda row: skip_lobby(row, 14), axis=1)
dataframes['data_2022-01-26_S1.csv'] = data


data = dataframes['data_2022-02-08_S1.csv']
data.loc[data['path'] == '2022-02-08_S1_pastaroniravioli_1291655662', 'lobbies_assigned_final'] \
    = data[data['path'] == '2022-02-08_S1_pastaroniravioli_1291655662'].apply(lambda row: skip_lobby(row, 3), axis=1)
dataframes['data_2022-02-08_S1.csv'] = data

# correct manually the assignments for one streamer where assignments did not work
data = dataframes['data_2022-02-21_S1.csv']
data = data[data['path'] != '2022-02-21_S1_pastaroniravioli_1305569006']
data.index = list(range(11))
dataframes['data_2022-02-21_S1.csv'] = data


# final check where not consecutive
for k in dataframes.keys():
    data = dataframes[k]
    for j in range(len(data)):
        lobbies = data['lobbies_assigned_final'][j]
        for i in range(len(lobbies) - 1):
            if lobbies[i] + 1 != lobbies[i + 1]:
                print(f'Streamer: {data["path"][j]}: Lobbies numbers not consecutive: {lobbies[i]} and {lobbies[i + 1]}')

# correct for two streamers after manual checking
data = dataframes['data_2022-01-20_S1.csv']
data['lobbies_assigned_final'] = data.apply(lambda row: remove_doubles(row), axis=1)
dataframes['data_2022-01-20_S1.csv'] = data

data = dataframes['data_2022-02-02_S1.csv']
data['lobbies_assigned_final'] = data.apply(lambda row: remove_doubles(row), axis=1)
dataframes['data_2022-02-02_S1.csv'] = data


# remove sessions where lobby extraction did not work
sessions_to_remove = ['data_2022-01-19_S1.csv', 'data_2022-01-20_S1.csv', 'data_2022-01-23_S1.csv', 'data_2022-01-23_S2.csv', 'data_2022-01-24_S1.csv', 'data_2022-02-03_S1.csv', 'data_2022-03-08_S1.csv']
for s in sessions_to_remove:
    del(dataframes[s])

# export final lobby assignments
with open(f'../../final_lobby_assignments.pkl', 'wb') as f:
    pickle.dump(dataframes, f)
