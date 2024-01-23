import pickle
import os
import pandas as pd
from datetime import timedelta
import numpy as np
import sympy as sp
from sympy import Matrix, solve_linear_system
from scipy.linalg import lstsq


sessions = os.listdir('streamer_dictionaries')
sessions_to_remove = ['2022-01-19_S1_streamer.pkl', '2022-01-20_S1_streamer.pkl', '2022-01-23_S1_streamer.pkl', '2022-01-23_S2_streamer.pkl', '2022-01-24_S1_streamer.pkl', '2022-02-03_S1_streamer.pkl', '2022-03-08_S1_streamer.pkl']
for s in sessions_to_remove:
    sessions.remove(s)

streamer_dictionaries = {}
for session in sessions:
    with open(f'streamer_dictionaries/{session}', 'rb') as f:
        loaded_dict = pickle.load(f)
    streamer_dictionaries[f'{session[:-13]}'] = loaded_dict


# add trustworthy lobby times for streamers without trustworthy lobby times
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

# delete manually one streamer where assignments did not work
del streamer_dictionaries['2022-02-21_S1']['2022-02-21_S1_pastaroniravioli_1305569006']


# pairwise dictionaries between streamers
distance_dic = {}
for session in streamer_dictionaries.keys():
    dic = streamer_dictionaries[session]
    distance_dic[session] = {}
    for streamer in dic.keys():
        distance_dic[session][streamer] = {}
        for partner in dic.keys():
            if partner != streamer:
                same_lobbies = list(set(streamer_dictionaries[session][streamer]['lobbies'].keys()) & set(streamer_dictionaries[session][partner]['lobbies'].keys()))
                distance_dic[session][streamer][partner] = []
                for l in same_lobbies:
                    duration_diff = abs((streamer_dictionaries[session][streamer]['lobbies'][l][1] - streamer_dictionaries[session][streamer]['lobbies'][l][0])  \
                        - (streamer_dictionaries[session][partner]['lobbies'][l][1] - streamer_dictionaries[session][partner]['lobbies'][l][0])).seconds
                    if duration_diff < 5:
                        # distance_dic[session][streamer][partner].append(streamer_dictionaries[session][streamer]['lobbies'][l])
                        # distance_dic[session][streamer][partner].append(streamer_dictionaries[session][partner]['lobbies'][l])
                        if streamer_dictionaries[session][streamer]['lobbies'][l][1] > streamer_dictionaries[session][partner]['lobbies'][l][1]:
                            distance_dic[session][streamer][partner].append((streamer_dictionaries[session][streamer]['lobbies'][l][1] - streamer_dictionaries[session][partner]['lobbies'][l][1]).total_seconds())
                        else:
                            distance_dic[session][streamer][partner].append(-(streamer_dictionaries[session][partner]['lobbies'][l][1] - streamer_dictionaries[session][streamer]['lobbies'][l][1]).total_seconds())
            else:
                distance_dic[session][streamer][partner] = []

"""
for se in distance_dic.keys():
    for st in distance_dic[se].keys():
        for pa in distance_dic[se][st].keys():
            for e in distance_dic[se][st][pa]:
                if e > 10:
                    print(f'{e} Session: {se}; Streamer: {st}; Partner: {pa}')
"""

for se in distance_dic.keys():
    for st in distance_dic[se].keys():
        for pa in distance_dic[se][st].keys():
            if len(distance_dic[se][st][pa]) != 0:
                distance_dic[se][st][pa] = [np.mean(distance_dic[se][st][pa])]


"""
for se in distance_dic.keys():
    for st in distance_dic[se].keys():
        rel = 0
        for pa in distance_dic[se][st].keys():
            rel = rel + len(distance_dic[se][st][pa])
        if rel == 0:
            print(f'Session: {se}; Streamer: {st}')
"""

# create matrix
# num_row = offsets i have -> non-empty lists / 2
# num_col = streamers i have in the session -> keys of dic
# fill with one's -> start with streamer_1 with all others, then streamer_2 with all others, ...
# use list to save which combination has already been added as row
solutions_dic = {}
for session in distance_dic.keys():
    num_rows = 0
    for st in distance_dic[session].keys():
        for pa in distance_dic[session][st].keys():
            num_rows = num_rows + len(distance_dic[session][st][pa])
    num_rows = int(num_rows / 2)
    num_col = len(distance_dic[session].keys())

    A = np.zeros((num_rows, num_col))

    row = 0
    displayed_relationships = []
    for ind_st, st in enumerate(distance_dic[session].keys()):
        for ind_pa, pa in enumerate(distance_dic[session][st].keys()):
            for e in distance_dic[session][st][pa]:
                if [st, pa] not in displayed_relationships and [pa, st] not in displayed_relationships:
                    A[row, ind_st] = 1
                    A[row, ind_pa] = -1
                    row += 1
                    displayed_relationships.append([st, pa])

    # create b
    b = np.zeros(num_rows)

    row = 0
    displayed_relationships = []
    for ind_st, st in enumerate(distance_dic[session].keys()):
        for ind_pa, pa in enumerate(distance_dic[session][st].keys()):
            for e in distance_dic[session][st][pa]:
                if [st, pa] not in displayed_relationships and [pa, st] not in displayed_relationships:
                    b[row] = e
                    row += 1
                    displayed_relationships.append([st, pa])

    """
    left = np.dot(A.transpose(), A)
    right = np.dot(A.transpose(), b)
    matrix = np.column_stack((left, right))

    symbols = [sp.symbols(f'x{i}') for i in range(1, len(list(distance_dic[session].keys()))+1)]
    system = Matrix(matrix)
    solution = solve_linear_system(system, *symbols)
    if solution is not None:
        relative_solution = {}
        for k in solution.keys():
            relative_solution[str(k)] = solution[k] - solution[x1]

        solutions_dic[session] = relative_solution
    """
    # use scipy least square solution
    x, residuals, rank, s = lstsq(A, b)
    relative_solution = {}
    for index, e in enumerate(x):
        relative_solution[list(distance_dic[session].keys())[index]] = e - x[0]
    solutions_dic[session] = relative_solution


# export solutions streamer offset dic
with open(f'streamer_offsets.pkl', 'wb') as f:
    pickle.dump(solutions_dic, f)

# export streamer dictionary containing the start time of each streamer
with open(f'streamer_start_times.pkl', 'wb') as f:
    pickle.dump(streamer_dictionaries, f)
