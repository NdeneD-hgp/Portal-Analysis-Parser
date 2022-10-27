import sys
import datetime
from math import nan
from queue import Empty
import pandas as pd
import numpy as np
import datetime as td
from dateutil import parser
from datetime import time, timedelta


"""
Test 1(find time spent on modules):
    *(Time spent = end - beginning )
    - find all modules
    - Check all module steps:
        TRACK:
            Check each individual track to see if they've been completed if any track that has
            been collected is not completed stop operations, and count the entire module that track is
            under as incomplete else move on:
                - Completion requirements(gather timestamp for beginning action and last action):
                    BEGINNING: Initial check for "viewed message"
                    END: finish track passing score

        CODE EXERCISE:
            Check each coding exercise to see if its been completed if it hasn't been completed
            stop the operation and mark the module as incomplete otherwise move on and check the coding
            for exercise for completion.
                - Completion requirements:
                    BEGINNING: "view code exercise"
                    END: "earn code answer score" && followed by "ace coding exercise"
                        - If earn... is found ace must directly follow afterwards otherwise
                          mark incomplete
                - Search is complete if check for "pass coding exercise" action is found are individual checks for coding exercise

        EXIT TICKET:
            Check for the initial, and final for completion if either are missing mark incomplete and stop the operation
                - Completion requirements:
                    BEGINNING: "view exit ticket prompt"
                    END: "create exit"
    VERIFY COMPLETION:
        BEGINNING: all tracks are completed successfully, exercises as well.
        END: exit tickets are done


"""


def parseTime(data):
    """
    PARAMETER: data(str) - 2022-10-20T19:16:20.078261Z
    RETURN: [[month, day, year], [hours, minutes, seconds]]
    """
    date = None
    time = None
    if not data.count('-') == 0:
        date = data[: data.index('T')].split('-')
    if not data.count(':') == 0:
        time = data[data.index('T') + 1: data.index('.')].split(':')

    return [date, time]


def calculateTime(time1, time2):
    timeObj1 = td.datetime(day=int(time1[0][2]), month=int(time1[0][1]), year=int(
        time1[0][0]), hour=int(time1[1][0]), minute=int(time1[1][1]), second=int(time1[1][2]))

    timeObj2 = td.datetime(day=int(time2[0][2]), month=int(time2[0][1]), year=int(
        time2[0][0]), hour=int(time2[1][0]), minute=int(time2[1][1]), second=int(time2[1][2]))

    return timeObj1 - timeObj2


def modulePassed(info, student_ids, modules):
    """
    Check module for completion:
        - Tracks
        - Exercises
        - Exit Ticket

    PARAMETER: info(dictionary) - student progress information, with times and completion of task
    RETURN: VOID
    """

    for id in student_ids:

        for x in modules:
            zero = datetime.timedelta(hours=0, minutes=0, seconds=0)
            t1 = None
            t2 = None
            t3 = None

            if info[id[0]][x[0]]['Tracks']['completed'] and info[id[0]][x[0]]['Exercises']['completed'] and info[id[0]][x[0]]['Exit']['completed']:
                info[id[0]][x[0]]['Module Completed'] = True

            track_time = info[id[0]][x[0]
                                     ]['Tracks']['time']['total'].split(':')
            exercise_time = info[id[0]][x[0]
                                        ]['Exercises']['time']['total'].split(':')
            exit_time = info[id[0]][x[0]]['Exit']['time'].split(":")

            if len(track_time) > 1:
                t1 = datetime.timedelta(
                    hours=int(track_time[0]), minutes=int(track_time[1]), seconds=int(track_time[2]))
            else:
                t1 = zero

            if len(exercise_time) > 1:
                t2 = datetime.timedelta(
                    hours=int(exercise_time[0]), minutes=int(exercise_time[1]), seconds=int(exercise_time[2]))
            else:
                t2 = zero

            if len(exit_time) > 1:
                t3 = datetime.timedelta(
                    hours=int(exit_time[0]), minutes=int(exit_time[1]), seconds=int(exit_time[2]))
            else:
                t3 = zero

            total = sum([t1, t2, t3], timedelta())
            info[id[0]][x[0]]['Module Time'] = str(total)


def main(args=None):

    file = None

    if args is None:
        print('Where\'s the file?')

    file = args

    # columns = genius_csv.head(0).columns.values

    genius_csv = pd.read_csv(file)

    student_ids = genius_csv[['student_id']].drop_duplicates().values
    modules = None

    info = dict()

    for student in student_ids:

        # Gathering modules done by specific student via id
        modules = genius_csv.set_index('student_id')[['cohort_name']].loc[student,
                                                                          :].drop_duplicates().dropna().values
        info.update({"%s" % student[0]: {}})

        for mod in modules:
            # Check tracks
            initial_track_time = None  # The start of the first track for a specific module
            initial_exercise_time = None  # The start of the first exercise for a specific module
            track_badge = None  # The completion time of a track if they earned a badge otherwise default is None, QN: Change to hold for datetime timedelta object for calculation purposes

            if type(mod[0]) is not float:

                # Updating dictionary to take format of incoming data for the modules content specific to the id

                info[student[0]].update({"%s" % mod[0]: {"Module Time": "", "Module Completed": False, "Tracks": {"amount": 0, "time": {"total": '', "individual": []},
                                                                                                                  "completed": False}, "Exercises": {"amount": 0, "time":
                                                                                                                                                     {"total": '', "individual": []}, "completed": False},  "Exit": {"time": "", "completed": False}}})
                # Getting all info pertaining to the associated student id
                index = genius_csv.set_index('student_id')
                df = index[['cohort_name', 'track_title', 'created_at',
                            'action_type', 'code_exercise_title']].loc[student, :].reset_index()

                # Gathering the current module information associated with the student id
                df = df.set_index('cohort_name').loc[mod, :].reset_index()

                # Gathering all tracks associated with the current module information, and associated student id
                tracks = df[['track_title', 'created_at',
                            'action_type']].dropna()

                # Gathering all exercises associated with the current module information, and associated student id
                exercise = df[['code_exercise_title',
                               'created_at', 'action_type']].dropna()

                # Selecting all information associated with the track_title column
                collection = df[df['track_title'].isna()]

                # Checking if exit ticket information exists within the selected collection
                exit_ticket = collection.loc[collection['action_type'].isin(
                    ['create exit ticket', 'view exit ticket prompt'])]

                # Updating dictionary with exit ticket information if the collection is not empty i.e it exists
                if not exit_ticket.empty:
                    start = parseTime(exit_ticket['created_at'].iloc[1])
                    end = parseTime(exit_ticket['created_at'].iloc[0])
                    ticket_diff = calculateTime(end, start)
                    info[student[0]][mod[0]]['Exit']['time'] = str(
                        ticket_diff)
                    info[student[0]][mod[0]]['Exit']['completed'] = True

                # All track related information such as: track titles, size, and whether its earned a completion badge
                track_names = tracks[['track_title']
                                     ].drop_duplicates().values
                track_size = track_names.size

                if not tracks.loc[tracks['action_type'].isin([
                        'earn badge'])].empty:
                    track_badge = tracks.loc[tracks['action_type'].isin([
                        'earn badge'])]['created_at'].iloc[0]

                elif not tracks.empty:
                    track_badge = tracks.loc[0, 'created_at']

                # # All coding exercise related information such as: code exercise titles, size, and whether its earned a completion badge
                exercise_names = exercise[[
                    'code_exercise_title']].drop_duplicates().values
                exercise_size = exercise_names.size

                if not exercise.loc[exercise['action_type'].isin(
                        ['pass all code exercises'])].empty:
                    exercise_badge = exercise.loc[exercise['action_type'].isin(
                        ['pass all code exercises'])]['created_at'].iloc[0]
                elif not exercise.empty:
                    exercise_badge = exercise.loc[0, 'created_at']

                # Updating dictionary with track/exercise sizes
                info[student[0]][mod[0]]['Tracks']['amount'] += track_size
                info[student[0]][mod[0]]['Exercises']['amount'] += exercise_size

                if not tracks.empty or not exercise.empty:

                    if track_size > 0 or exercise_size > 0:

                        # Track and coding exercise names
                        names = [track_names, exercise_names]
                        data = [tracks, exercise, 'track_title',
                                'code_exercise_title']

                        # Quick loop for iterating over specific tracks or exercises
                        for k in range(len(names)):
                            # Will loop through the stored array of names
                            for i, x in enumerate(names[k]):
                                test = data[k].set_index(
                                    data[k + 2]).loc[x, 'created_at':]  # Gathering the times associated with track or coding exercises

                                # Parsing time from the tracks or coding exercises to be used for calculation later
                                start = parseTime(test.tail(1).iloc[0, 0])
                                end = parseTime(test.head(1).iloc[0, 0])

                                # Checking if this is the starting time of track or coding exercises
                                if names[k].size - 1 == i:
                                    if k == 0:
                                        initial_track_time = start
                                    else:
                                        initial_exercise_time = start

                                time_spent = calculateTime(end, start)

                                # Checking for track information update
                                if k == 0:
                                    info[student[0]][mod[0]]['Tracks']['time']['individual'].insert(
                                        0, str(time_spent))
                                # Checking for exercise information update
                                else:
                                    info[student[0]][mod[0]]['Exercises']['time']['individual'].insert(
                                        0, str(time_spent))

            # This currently does not work needs an update to work properly with changes made
            if mod is nan:
                completion = df.set_index(
                    'action_type').loc['finish track passing score', 'track_title':'created_at']
                print(completion)

            # Calculate all tracks time for a total time on tracks within module
            if initial_track_time and track_badge:

                end_time = parseTime(track_badge)
                track_total = calculateTime(end_time, initial_track_time)
                info[student[0]][mod[0]]['Tracks']['time']['total'] = str(
                    track_total)
                info[student[0]][mod[0]]['Tracks']['completed'] = True

            # Calculate all coding exercise time for a total time on coding exercises within module
            if initial_exercise_time and exercise_badge:
                end_time = parseTime(exercise_badge)
                exercise_total = calculateTime(
                    end_time, initial_exercise_time)
                info[student[0]][mod[0]]['Exercises']['time']['total'] = str(
                    exercise_total)
                info[student[0]][mod[0]]['Exercises']['completed'] = True

        modulePassed(info, student_ids, modules)
        print(info)


def plotTimes():
    """
    TODO: Create a function that takes a list of modules for the student and returns plot of the times, and modules
    TODO: Pie chart and line graph
    """
    pass


main(sys.argv[1])
