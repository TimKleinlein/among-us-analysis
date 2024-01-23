import pickle
import os
import pandas as pd
from datetime import timedelta

# first create streamer dictionary because later start time of main streamers is needed
sessions = os.listdir('streamer_dictionaries')
sessions_to_remove = ['2022-01-19_S1_streamer.pkl', '2022-01-20_S1_streamer.pkl', '2022-01-23_S1_streamer.pkl', '2022-01-23_S2_streamer.pkl', '2022-01-24_S1_streamer.pkl', '2022-02-03_S1_streamer.pkl', '2022-03-08_S1_streamer.pkl']
for s in sessions_to_remove:
    sessions.remove(s)

streamer_dictionaries = {}
for session in sessions:
    with open(f'streamer_dictionaries/{session}', 'rb') as f:
        loaded_dict = pickle.load(f)
    streamer_dictionaries[f'{session[:-13]}'] = loaded_dict

# now import lobbies dictionaries
sessions = os.listdir('lobbies_dictionaries')
sessions_to_remove = ['2022-01-19_S1_lobbies.pkl', '2022-01-20_S1_lobbies.pkl', '2022-01-23_S1_lobbies.pkl', '2022-01-23_S2_lobbies.pkl', '2022-01-24_S1_lobbies.pkl', '2022-02-03_S1_lobbies.pkl', '2022-03-08_S1_lobbies.pkl']
for s in sessions_to_remove:
    sessions.remove(s)

lobbies_dictionaries = {}
for session in sessions:
    with open(f'lobbies_dictionaries/{session}', 'rb') as f:
        loaded_dict = pickle.load(f)
    lobbies_dictionaries[f'{session[:-12]}'] = loaded_dict

# add trustworthy lobby times for lobbies without trustworthy lobby times
df = pd.read_excel('Results.xlsx', sheet_name='Lobbies_Trustworthy_Lobbies')
df['Session'] = df['Streamer'].apply(lambda x: x[:13])
def enter_lobby_in_dictionary(row):
    streamer_start_time = list(streamer_dictionaries[row['Session']][row['Streamer']]['start_time'])[0]
    lobby_start = row['Trustworthy lobby start']
    lobby_end = row['Trustworthy lobby end']
    start_time = streamer_start_time + timedelta(hours=lobby_start.hour, minutes=lobby_start.minute, seconds=lobby_start.second)
    end_time = streamer_start_time + timedelta(hours=lobby_end.hour, minutes=lobby_end.minute, seconds=lobby_end.second)

    # insert into lobbies dictionary
    lobbies_dictionaries[row['Session']][row['Trustworthy lobby number']][row['Streamer']] = [start_time, end_time]

df.apply(lambda row: enter_lobby_in_dictionary(row), axis=1)

"""
# add trustworthy lobby times for lobbies without trustworthy lobby times
df = pd.read_excel('Results.xlsx', sheet_name='Streamer_Trustworthy_Lobbies')
for session in streamer_dictionaries.keys():
    dic = streamer_dictionaries[session]
    for streamer in dic.keys():
        if streamer in list(df['Streamer']):
            lobby_number = list(df[df['Streamer'] == streamer]['Trustworthy lobby number'])[0]
            lobby_start = list(df[df['Streamer'] == streamer]['Trustworthy lobby start'])[0]
            lobby_end = list(df[df['Streamer'] == streamer]['Trustworthy lobby end'])[0]
            start_time = list(streamer_dictionaries[session][streamer]['start_time'])[0] + timedelta(hours=lobby_start.hour, minutes=lobby_start.minute, seconds=lobby_start.second)
            end_time = list(streamer_dictionaries[session][streamer]['start_time'])[0] + timedelta(hours=lobby_end.hour, minutes=lobby_end.minute, seconds=lobby_end.second)

            streamer_dictionaries[session][streamer]['lobbies'][lobby_number] = [start_time, end_time]


# add trustworthy lobby times for main streamers from lobbies without trustworthy times
df = pd.read_excel('Results.xlsx', sheet_name='Main_Streamer_Lobbies')
for session in streamer_dictionaries.keys():
    dic = streamer_dictionaries[session]
    for streamer in dic.keys():
        if streamer in list(df['Streamer']):
            lobby_numbers = list(df[df['Streamer'] == streamer]['Trustworthy lobby number'])
            lobby_starts = list(df[df['Streamer'] == streamer]['Trustworthy lobby start'])
            lobby_ends = list(df[df['Streamer'] == streamer]['Trustworthy lobby end'])
            for index, num in enumerate(lobby_numbers):
                start_time = list(streamer_dictionaries[session][streamer]['start_time'])[0] + timedelta(hours=lobby_starts[index].hour, minutes=lobby_starts[index].minute, seconds=lobby_starts[index].second)
                end_time = list(streamer_dictionaries[session][streamer]['start_time'])[0] + timedelta(hours=lobby_ends[index].hour, minutes=lobby_ends[index].minute, seconds=lobby_ends[index].second)

                streamer_dictionaries[session][streamer]['lobbies'][num] = [start_time, end_time]
"""

# check if remaining lobbies without trustworthy time
for session in lobbies_dictionaries.keys():
    for lobby in lobbies_dictionaries[session].keys():
        if len(lobbies_dictionaries[session][lobby]) == 0:
            print(f'{session} - {lobby}')

# export lobbies dic
with open(f'trustworthy_lobbies.pkl', 'wb') as f:
    pickle.dump(lobbies_dictionaries, f)
