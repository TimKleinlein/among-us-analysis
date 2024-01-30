import pickle
import os
import pandas as pd
from datetime import timedelta

# first import streamer dictionary because later start time of main streamers is needed
sessions = os.listdir('../../data/initial_synchronization_output/streamer_dictionaries')
sessions_to_remove = ['2022-01-19_S1_streamer.pkl', '2022-01-20_S1_streamer.pkl', '2022-01-23_S1_streamer.pkl', '2022-01-23_S2_streamer.pkl', '2022-01-24_S1_streamer.pkl', '2022-02-03_S1_streamer.pkl', '2022-03-08_S1_streamer.pkl']
for s in sessions_to_remove:
    sessions.remove(s)

streamer_dictionaries = {}
for session in sessions:
    with open(f'../../data/initial_synchronization_output/streamer_dictionaries/{session}', 'rb') as f:
        loaded_dict = pickle.load(f)
    streamer_dictionaries[f'{session[:-13]}'] = loaded_dict

# now import lobbies dictionaries storing the trustworthy lobby times for each lobby
sessions = os.listdir('../../data/initial_synchronization_output/lobbies_dictionaries')
sessions_to_remove = ['2022-01-19_S1_lobbies.pkl', '2022-01-20_S1_lobbies.pkl', '2022-01-23_S1_lobbies.pkl', '2022-01-23_S2_lobbies.pkl', '2022-01-24_S1_lobbies.pkl', '2022-02-03_S1_lobbies.pkl', '2022-03-08_S1_lobbies.pkl']
for s in sessions_to_remove:
    sessions.remove(s)

lobbies_dictionaries = {}
for session in sessions:
    with open(f'../../data/initial_synchronization_output/lobbies_dictionaries/{session}', 'rb') as f:
        loaded_dict = pickle.load(f)
    lobbies_dictionaries[f'{session[:-12]}'] = loaded_dict

# add trustworthy lobby times for lobbies without trustworthy lobby times to trustworthy lobby dictionary using the times gained from the manual lobby extraction
df = pd.read_excel('../../data/manual_lobby_extraction/Results.xlsx', sheet_name='Lobbies_Trustworthy_Lobbies')
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


# check if remaining lobbies without trustworthy time
for session in lobbies_dictionaries.keys():
    for lobby in lobbies_dictionaries[session].keys():
        if len(lobbies_dictionaries[session][lobby]) == 0:
            print(f'{session} - {lobby}')

# export lobbies dic with trustworthy lobby times for each lobby to be used in combine_results.py
with open(f'../../data/final_synchronization_output/trustworthy_lobbies.pkl', 'wb') as f:
    pickle.dump(lobbies_dictionaries, f)
