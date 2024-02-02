import pickle
import os
from datetime import timedelta

# import streamer offsets
with open(f'../../data/final_synchronization_output/streamer_offsets.pkl', 'rb') as f:
    streamer_offsets = pickle.load(f)

# import trustworthy lobby times
with open(f'../../data/final_synchronization_output/trustworthy_lobbies.pkl', 'rb') as f:
    trustworthy_lobbies = pickle.load(f)

# import final lobby assignments
with open(f'../../data/final_synchronization_output/final_lobby_assignments.pkl', 'rb') as f:
    participated_lobbies = pickle.load(f)

# for each session build ranking dictionary storing which streamer has the most trustworthy times
# -> if for a lobby more trustworthy times exist his time is taken as trustworthy lobby time for this lobby
ranking_dic = {}
for session in trustworthy_lobbies.keys():
    dic = trustworthy_lobbies[session]
    ranking_dic[session] = {}
    for lobby in dic.keys():
        for streamer in dic[lobby].keys():
            if streamer in ranking_dic[session].keys():
                ranking_dic[session][streamer] = ranking_dic[session][streamer] + 1
            else:
                ranking_dic[session][streamer] = 1

# go through all sessions and all lobbies and choose lobby time from the preferred streamer
for session in trustworthy_lobbies.keys():
    dic = trustworthy_lobbies[session]
    for lobby in dic.keys():
        if len(dic[lobby]) > 1:
            winning_streamer = list(dic[lobby].keys())[0]
            for s in dic[lobby].keys():
                if ranking_dic[session][s] > ranking_dic[session][winning_streamer]:
                    winning_streamer = s
            all_streamers_without_winner = list(dic[lobby].keys())
            all_streamers_without_winner.remove(winning_streamer)
            for s in all_streamers_without_winner:
                del dic[lobby][s]


# get final lobby times for all streamers and store in dictionary: go over all lobbies and for all streamers who participated
# in the lobby get their lobby time by adding their offset to the trustworthy streamer to this streamers lobby time
final_lobby_times = {}
for session in trustworthy_lobbies.keys():
    dic = trustworthy_lobbies[session]
    final_lobby_times[session] = {}
    for lobby in dic.keys():
        final_lobby_times[session][lobby] = {}
        # get trustworthy streamer and his time
        trustworthy_streamer = list(dic[lobby].keys())[0]
        trustworthy_time = dic[lobby][trustworthy_streamer]
        # insert correct time (trustworthy lobby time + offset) for all streamers who participated in the lobby
        for streamer in participated_lobbies[f'data_{session}.csv']['path']:
            if lobby in list(participated_lobbies[f'data_{session}.csv'][participated_lobbies[f'data_{session}.csv']['path'] == streamer]['lobbies_assigned_final'])[0]:
                offset = streamer_offsets[session][streamer] - streamer_offsets[session][trustworthy_streamer]
                correct_times = [trustworthy_time[0] + timedelta(seconds=offset), trustworthy_time[1] + timedelta(seconds=offset)]
                final_lobby_times[session][lobby][streamer] = correct_times

# subtract start time from each streamer to have lobby times in srt format
with open(f'../../data/final_synchronization_output/streamer_start_times.pkl', 'rb') as f:
    streamer_start_times = pickle.load(f)
for session in final_lobby_times.keys():
    for lobby in final_lobby_times[session].keys():
        for streamer in final_lobby_times[session][lobby].keys():
            final_lobby_times[session][lobby][streamer][0] = final_lobby_times[session][lobby][streamer][0] - list(streamer_start_times[session][streamer]['start_time'])[0]
            final_lobby_times[session][lobby][streamer][1] = final_lobby_times[session][lobby][streamer][1] - list(streamer_start_times[session][streamer]['start_time'])[0]

# export final lobby times as pkl file
with open(f'../../data/final_synchronization_output/final_lobby_times.pkl', 'wb') as f:
    pickle.dump(final_lobby_times, f)
