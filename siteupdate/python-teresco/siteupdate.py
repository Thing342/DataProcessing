#!/usr/bin/env python3
# Travel Mapping Project, Jim Teresco, 2015-2018
"""Python code to read .csv and .wpt files and prepare for
adding to the Travel Mapping Project database.

(c) 2015-2018, Jim Teresco

This module defines classes to represent the contents of a
.csv file that lists the highways within a system, and a
.wpt file that lists the waypoints for a given highway.
"""

import argparse
import datetime
import math
import os
import re
import sys
import threading

import nmp
from datachecks import DatacheckEntry
from graphs import HighwayGraph, GraphListEntry, PlaceRadius
from travelers import TravelerList
from util import ErrorList, ElapsedTime
from quadtree import WaypointQuadtree
from wpt import HighwaySystem


def format_clinched_mi(clinched, total):
    """return a nicely-formatted string for a given number of miles
    clinched and total miles, including percentage"""
    percentage = "-.-%"
    if total != 0.0:
        percentage = "({0:.1f}%)".format(100 * clinched / total)
    return "{0:.2f}".format(clinched) + " of {0:.2f}".format(total) + \
           " mi " + percentage


# 
# Execution code starts here
#
# start a timer for including elapsed time reports in messages
et = ElapsedTime()

# create a ErrorList
el = ErrorList()

# argument parsing
#
parser = argparse.ArgumentParser(
    description="Create SQL, stats, graphs, and log files from highway and user data for the Travel Mapping project.")
parser.add_argument("-w", "--highwaydatapath", default="../../../HighwayData", \
                    help="path to the root of the highway data directory structure")
parser.add_argument("-s", "--systemsfile", default="systems.csv", \
                    help="file of highway systems to include")
parser.add_argument("-u", "--userlistfilepath", default="../../../UserData/list_files", \
                    help="path to the user list file data")
parser.add_argument("-d", "--databasename", default="TravelMapping", \
                    help="Database name for .sql file name")
parser.add_argument("-l", "--logfilepath", default=".",
                    help="Path to write log files, which should have a \"users\" subdirectory")
parser.add_argument("-c", "--csvstatfilepath", default=".", help="Path to write csv statistics files")
parser.add_argument("-g", "--graphfilepath", default=".", help="Path to write graph format data files")
parser.add_argument("-k", "--skipgraphs", action="store_true", help="Turn off generation of graph files")
parser.add_argument("-n", "--nmpmergepath", default="",
                    help="Path to write data with NMPs merged (generated only if specified)")
parser.add_argument("-U", "--userlist", default=None, nargs="+",
                    help="For Development: list of users to use in dataset")
parser.add_argument("-t", "--numthreads", default="4", help="Number of threads to use for concurrent tasks")
parser.add_argument("-e", "--errorcheck", action="store_true",
                    help="Run only the subset of the process needed to verify highway data changes")
args = parser.parse_args()

#
# Get list of travelers in the system
traveler_ids = args.userlist
traveler_ids = os.listdir(args.userlistfilepath) if traveler_ids is None else (id + ".list" for id in traveler_ids)

# number of threads to use
num_threads = int(args.numthreads)

# read region, country, continent descriptions
print(et.et() + "Reading region, country, and continent descriptions.")

continents = []
try:
    file = open(args.highwaydatapath + "/continents.csv", "rt", encoding='utf-8')
except OSError as e:
    el.add_error(str(e))
else:
    lines = file.readlines()
    file.close()
    lines.pop(0)  # ignore header line
    for line in lines:
        fields = line.rstrip('\n').split(";")
        if len(fields) != 2:
            el.add_error("Could not parse continents.csv line: " + line)
            continue
        continents.append(fields)

countries = []
try:
    file = open(args.highwaydatapath + "/countries.csv", "rt", encoding='utf-8')
except OSError as e:
    el.add_error(str(e))
else:
    lines = file.readlines()
    file.close()
    lines.pop(0)  # ignore header line
    for line in lines:
        fields = line.rstrip('\n').split(";")
        if len(fields) != 2:
            el.add_error("Could not parse countries.csv line: " + line)
            continue
        countries.append(fields)

all_regions = []
try:
    file = open(args.highwaydatapath + "/regions.csv", "rt", encoding='utf-8')
except OSError as e:
    el.add_error(str(e))
else:
    lines = file.readlines()
    file.close()
    lines.pop(0)  # ignore header line
    for line in lines:
        fields = line.rstrip('\n').split(";")
        if len(fields) != 5:
            el.add_error("Could not parse regions.csv line: " + line)
            continue
        # look up country and continent, add index into those arrays
        # in case they're needed for lookups later (not needed for DB)
        for i in range(len(countries)):
            country = countries[i][0]
            if country == fields[2]:
                fields.append(i)
                break
        if len(fields) != 6:
            el.add_error("Could not find country matching regions.csv line: " + line)
            continue
        for i in range(len(continents)):
            continent = continents[i][0]
            if continent == fields[3]:
                fields.append(i)
                break
        if len(fields) != 7:
            el.add_error("Could not find continent matching regions.csv line: " + line)
            continue
        all_regions.append(fields)

# Create a list of HighwaySystem objects, one per system in systems.csv file
highway_systems = []
print(et.et() + "Reading systems list in " + args.highwaydatapath + "/" + args.systemsfile + ".  ", end="", flush=True)
try:
    file = open(args.highwaydatapath + "/" + args.systemsfile, "rt", encoding='utf-8')
except OSError as e:
    el.add_error(str(e))
else:
    lines = file.readlines()
    file.close()
    lines.pop(0)  # ignore header line for now
    ignoring = []
    for line in lines:
        if line.startswith('#'):
            ignoring.append("Ignored comment in " + args.systemsfile + ": " + line.rstrip('\n'))
            continue
        fields = line.rstrip('\n').split(";")
        if len(fields) != 6:
            el.add_error("Could not parse " + args.systemsfile + " line: " + line)
            continue
        print(fields[0] + ".", end="", flush=True)
        hs = HighwaySystem(fields[0], fields[1],
                           fields[2].replace("'", "''"),
                           fields[3], fields[4], fields[5], el,
                           args.highwaydatapath + "/hwy_data/_systems")
        highway_systems.append(hs)
    print("")
    # print at the end the lines ignored
    for line in ignoring:
        print(line)

# list for datacheck errors that we will need later
datacheckerrors = []

# check for duplicate root entries among Route and ConnectedRoute
# data in all highway systems
print(et.et() + "Checking for duplicate list names in routes, roots in routes and connected routes.", flush=True)
roots = set()
list_names = set()
duplicate_list_names = set()
for h in highway_systems:
    for r in h.route_list:
        if r.root in roots:
            el.add_error("Duplicate root in route lists: " + r.root)
        else:
            roots.add(r.root)
        list_name = r.region + ' ' + r.list_entry_name()
        if list_name in list_names:
            duplicate_list_names.add(list_name)
        else:
            list_names.add(list_name)

con_roots = set()
for h in highway_systems:
    for cr in h.con_route_list:
        for r in cr.roots:
            if r.root in con_roots:
                el.add_error("Duplicate root in con_route lists: " + r.root)
            else:
                con_roots.add(r.root)

# Make sure every route was listed as a part of some connected route
if len(roots) == len(con_roots):
    print("Check passed: same number of routes as connected route roots. " + str(len(roots)))
else:
    el.add_error("Check FAILED: " + str(len(roots)) + " routes != " + str(len(con_roots)) + " connected route roots.")
    roots = roots - con_roots
    # there will be some leftovers, let's look up their routes to make
    # an error report entry (not worried about efficiency as there would
    # only be a few in reasonable cases)
    num_found = 0
    for h in highway_systems:
        for r in h.route_list:
            for lr in roots:
                if lr == r.root:
                    el.add_error("route " + lr + " not matched by any connected route root.")
                    num_found += 1
                    break
    print("Added " + str(num_found) + " ROUTE_NOT_IN_CONNECTED error entries.")

# report any duplicate list names as errors
if len(duplicate_list_names) > 0:
    print("Found " + str(len(duplicate_list_names)) + " DUPLICATE_LIST_NAME case(s).")
    for d in duplicate_list_names:
        el.add_error("Duplicate list name: " + d)
else:
    print("No duplicate list names found.")

# write file mapping CHM datacheck route lists to root (commented out,
# unlikely needed now)
# print(et.et() + "Writing CHM datacheck to TravelMapping route pairings.")
# file = open(args.csvstatfilepath + "/routepairings.csv","wt")
# for h in highway_systems:
#    for r in h.route_list:
#        file.write(r.region + " " + r.list_entry_name() + ";" + r.root + "\n")
# file.close()

# For tracking whether any .wpt files are in the directory tree
# that do not have a .csv file entry that causes them to be
# read into the data
print(et.et() + "Finding all .wpt files. ", end="", flush=True)
all_wpt_files = []
for dir, sub, files in os.walk(args.highwaydatapath + "/hwy_data"):
    for file in files:
        if file.endswith('.wpt') and '_boundaries' not in dir:
            all_wpt_files.append(dir + "/" + file)
print(str(len(all_wpt_files)) + " files found.")

# For finding colocated Waypoints and concurrent segments, we have
# quadtree of all Waypoints in existence to find them efficiently
all_waypoints = WaypointQuadtree(-90, -180, 90, 180)
all_waypoints_lock = threading.Lock()

print(et.et() + "Reading waypoints for all routes.")


# Next, read all of the .wpt files for each HighwaySystem
def read_wpts_for_highway_system(h):
    print(h.systemname, end="", flush=True)
    for r in h.route_list:
        # get full path to remove from all_wpt_files list
        wpt_path = args.highwaydatapath + "/hwy_data" + "/" + r.region + "/" + r.system.systemname + "/" + r.root + ".wpt"
        if wpt_path in all_wpt_files:
            all_wpt_files.remove(wpt_path)
        r.read_wpt(all_waypoints, all_waypoints_lock, datacheckerrors,
                   el, args.highwaydatapath + "/hwy_data")
        if len(r.point_list) < 2:
            el.add_error("Route contains fewer than 2 points: " + str(r))
        print(".", end="", flush=True)
        # print(str(r))
        # r.print_route()
    print("!", flush=True)


# set up for threaded processing of highway systems
class ReadWptThread(threading.Thread):

    def __init__(self, id, hs_list, lock):
        threading.Thread.__init__(self)
        self.id = id
        self.hs_list = hs_list
        self.lock = lock

    def run(self):
        # print("Starting ReadWptThread " + str(self.id) + " lock is " + str(self.lock))
        while True:
            self.lock.acquire(True)
            # print("Thread " + str(self.id) + " with len(self.hs_list)=" + str(len(self.hs_list)))
            if len(self.hs_list) == 0:
                self.lock.release()
                break
            h = self.hs_list.pop()
            self.lock.release()
            # print("Thread " + str(self.id) + " assigned " + str(h))
            read_wpts_for_highway_system(h)

        # print("Exiting ReadWptThread " + str(self.id))


hs_lock = threading.Lock()
# print("Created lock: " + str(hs_lock))
hs = highway_systems[:]
hs.reverse()
thread_list = []
# create threads
for i in range(num_threads):
    thread_list.append(ReadWptThread(i, hs, hs_lock))

# start threads
for t in thread_list:
    t.start()

# wait for threads
for t in thread_list:
    t.join()

# for h in highway_systems:
#    read_wpts_for_highway_system(h)

print(et.et() + "Sorting waypoints in Quadtree.")
all_waypoints.sort()

print(et.et() + "Sorting colocated point lists.")
for w in all_waypoints.point_list():
    if w.colocated is not None:
        w.colocated.sort(key=lambda waypoint: waypoint.route.root + "@" + waypoint.label)

print(et.et() + "Finding unprocessed wpt files.", flush=True)
unprocessedfile = open(args.logfilepath + '/unprocessedwpts.log', 'w', encoding='utf-8')
if len(all_wpt_files) > 0:
    print(str(len(all_wpt_files)) + " .wpt files in " + args.highwaydatapath +
          "/hwy_data not processed, see unprocessedwpts.log.")
    for file in all_wpt_files:
        unprocessedfile.write(file[file.find('hwy_data'):] + '\n')
else:
    print("All .wpt files in " + args.highwaydatapath +
          "/hwy_data processed.")
unprocessedfile.close()

# Near-miss point log
print(et.et() + "Near-miss point log and tm-master.nmp file.", flush=True)
nmp.do_nmp()

# Create hash table for faster lookup of routes by list file name
print(et.et() + "Creating route hash table for list processing:", flush=True)
route_hash = dict()
for h in highway_systems:
    for r in h.route_list:
        route_hash[(r.region + ' ' + r.list_entry_name()).lower()] = r
        for a in r.alt_route_names:
            route_hash[(r.region + ' ' + a).lower()] = r

# Create a list of TravelerList objects, one per person
traveler_lists = []

print(et.et() + "Processing traveler list files:", end="", flush=True)
for t in traveler_ids:
    if t.endswith('.list'):
        print(" " + t, end="", flush=True)
        traveler_lists.append(TravelerList(t, route_hash, args.userlistfilepath))
print(" processed " + str(len(traveler_lists)) + " traveler list files.")
traveler_lists.sort(key=lambda TravelerList: TravelerList.traveler_name)

# Read updates.csv file, just keep in the fields array for now since we're
# just going to drop this into the DB later anyway
updates = []
print(et.et() + "Reading updates file.  ", end="", flush=True)
with open(args.highwaydatapath + "/updates.csv", "rt", encoding='UTF-8') as file:
    lines = file.readlines()
file.close()

lines.pop(0)  # ignore header line
for line in lines:
    fields = line.rstrip('\n').split(';')
    if len(fields) != 5:
        print("Could not parse updates.csv line: " + line)
        continue
    updates.append(fields)
print("")

# Same plan for systemupdates.csv file, again just keep in the fields
# array for now since we're just going to drop this into the DB later
# anyway
systemupdates = []
print(et.et() + "Reading systemupdates file.  ", end="", flush=True)
with open(args.highwaydatapath + "/systemupdates.csv", "rt", encoding='UTF-8') as file:
    lines = file.readlines()
file.close()

lines.pop(0)  # ignore header line
for line in lines:
    fields = line.rstrip('\n').split(';')
    if len(fields) != 5:
        print("Could not parse systemupdates.csv line: " + line)
        continue
    systemupdates.append(fields)
print("")

# write log file for points in use -- might be more useful in the DB later,
# or maybe in another format
print(et.et() + "Writing points in use log.")
inusefile = open(args.logfilepath + '/pointsinuse.log', 'w', encoding='UTF-8')
inusefile.write("Log file created at: " + str(datetime.datetime.now()) + "\n")
for h in highway_systems:
    for r in h.route_list:
        if len(r.labels_in_use) > 0:
            inusefile.write(r.root + "(" + str(len(r.point_list)) + "):")
            for label in sorted(r.labels_in_use):
                inusefile.write(" " + label)
            inusefile.write("\n")
            r.labels_in_use = None
inusefile.close()

# write log file for alt labels not in use
print(et.et() + "Writing unused alt labels log.")
unusedfile = open(args.logfilepath + '/unusedaltlabels.log', 'w', encoding='UTF-8')
unusedfile.write("Log file created at: " + str(datetime.datetime.now()) + "\n")
total_unused_alt_labels = 0
for h in highway_systems:
    for r in h.route_list:
        if len(r.unused_alt_labels) > 0:
            total_unused_alt_labels += len(r.unused_alt_labels)
            unusedfile.write(r.root + "(" + str(len(r.unused_alt_labels)) + "):")
            for label in sorted(r.unused_alt_labels):
                unusedfile.write(" " + label)
            unusedfile.write("\n")
            r.unused_alt_labels = None
unusedfile.write("Total: " + str(total_unused_alt_labels) + "\n")
unusedfile.close()

# concurrency detection -- will augment our structure with list of concurrent
# segments with each segment (that has a concurrency)
print(et.et() + "Concurrent segment detection.", end="", flush=True)
concurrencyfile = open(args.logfilepath + '/concurrencies.log', 'w', encoding='UTF-8')
concurrencyfile.write("Log file created at: " + str(datetime.datetime.now()) + "\n")
for h in highway_systems:
    print(".", end="", flush=True)
    for r in h.route_list:
        for s in r.segment_list:
            if s.waypoint1.colocated is not None and s.waypoint2.colocated is not None:
                for w1 in s.waypoint1.colocated:
                    if w1.route is not r:
                        for w2 in s.waypoint2.colocated:
                            if w1.route is w2.route:
                                other = w1.route.find_segment_by_waypoints(w1, w2)
                                if other is not None:
                                    if s.concurrent is None:
                                        s.concurrent = []
                                        other.concurrent = s.concurrent
                                        s.concurrent.append(s)
                                        s.concurrent.append(other)
                                        concurrencyfile.write(
                                            "New concurrency [" + str(s) + "][" + str(other) + "] (" + str(
                                                len(s.concurrent)) + ")\n")
                                    else:
                                        other.concurrent = s.concurrent
                                        if other not in s.concurrent:
                                            s.concurrent.append(other)
                                            # concurrencyfile.write("Added concurrency [" + str(s) + "]-[" + str(other) + "] ("+ str(len(s.concurrent)) + ")\n")
                                            concurrencyfile.write("Extended concurrency ")
                                            for x in s.concurrent:
                                                concurrencyfile.write("[" + str(x) + "]")
                                            concurrencyfile.write(" (" + str(len(s.concurrent)) + ")\n")
print("!")

# now augment any traveler clinched segments for concurrencies

print(et.et() + "Augmenting travelers for detected concurrent segments.", end="", flush=True)
for t in traveler_lists:
    print(".", end="", flush=True)
    for s in t.clinched_segments:
        if s.concurrent is not None:
            for hs in s.concurrent:
                if hs.route.system.active_or_preview() and hs.add_clinched_by(t):
                    concurrencyfile.write(
                        "Concurrency augment for traveler " + t.traveler_name + ": [" + str(hs) + "] based on [" + str(
                            s) + "]\n")
print("!")
concurrencyfile.close()

# compute lots of stats, first total mileage by route, system, overall, where
# system and overall are stored in dictionaries by region
print(et.et() + "Computing stats.", end="", flush=True)
# now also keeping separate totals for active only, active+preview,
# and all for overall (not needed for system, as a system falls into just
# one of these categories)
active_only_mileage_by_region = dict()
active_preview_mileage_by_region = dict()
overall_mileage_by_region = dict()
for h in highway_systems:
    print(".", end="", flush=True)
    for r in h.route_list:
        for s in r.segment_list:
            segment_length = s.length()
            # always add the segment mileage to the route
            r.mileage += segment_length
            # but we do need to check for concurrencies for others
            system_concurrency_count = 1
            active_only_concurrency_count = 1
            active_preview_concurrency_count = 1
            overall_concurrency_count = 1
            if s.concurrent is not None:
                for other in s.concurrent:
                    if other != s:
                        overall_concurrency_count += 1
                        if other.route.system.active_or_preview():
                            active_preview_concurrency_count += 1
                            if other.route.system.active():
                                active_only_concurrency_count += 1
                        if other.route.system == r.system:
                            system_concurrency_count += 1
            # we know how many times this segment will be encountered
            # in both the system and overall/active+preview/active-only
            # routes, so let's add in the appropriate (possibly fractional)
            # mileage to the overall totals and to the system categorized
            # by its region
            #
            # first, overall mileage for this region, add to overall
            # if an entry already exists, create entry if not
            if r.region in overall_mileage_by_region:
                overall_mileage_by_region[r.region] = overall_mileage_by_region[r.region] + \
                                                      segment_length / overall_concurrency_count
            else:
                overall_mileage_by_region[r.region] = segment_length / overall_concurrency_count

            # next, same thing for active_preview mileage for the region,
            # if active or preview
            if r.system.active_or_preview():
                if r.region in active_preview_mileage_by_region:
                    active_preview_mileage_by_region[r.region] = active_preview_mileage_by_region[r.region] + \
                                                                 segment_length / active_preview_concurrency_count
                else:
                    active_preview_mileage_by_region[r.region] = segment_length / active_preview_concurrency_count

            # now same thing for active_only mileage for the region,
            # if active
            if r.system.active():
                if r.region in active_only_mileage_by_region:
                    active_only_mileage_by_region[r.region] = active_only_mileage_by_region[r.region] + \
                                                              segment_length / active_only_concurrency_count
                else:
                    active_only_mileage_by_region[r.region] = segment_length / active_only_concurrency_count

            # now we move on to totals by region, only the
            # overall since an entire highway system must be
            # at the same level
            if r.region in h.mileage_by_region:
                h.mileage_by_region[r.region] = h.mileage_by_region[r.region] + \
                                                segment_length / system_concurrency_count
            else:
                h.mileage_by_region[r.region] = segment_length / system_concurrency_count

            # that's it for overall stats, now credit all travelers
            # who have clinched this segment in their stats
            for t in s.clinched_by:
                # credit active+preview for this region, which it must be
                # if this segment is clinched by anyone but still check
                # in case a concurrency detection might otherwise credit
                # a traveler with miles in a devel system
                if r.system.active_or_preview():
                    if r.region in t.active_preview_mileage_by_region:
                        t.active_preview_mileage_by_region[r.region] = t.active_preview_mileage_by_region[r.region] + \
                                                                       segment_length / active_preview_concurrency_count
                    else:
                        t.active_preview_mileage_by_region[r.region] = segment_length / active_preview_concurrency_count

                # credit active only for this region
                if r.system.active():
                    if r.region in t.active_only_mileage_by_region:
                        t.active_only_mileage_by_region[r.region] = t.active_only_mileage_by_region[r.region] + \
                                                                    segment_length / active_only_concurrency_count
                    else:
                        t.active_only_mileage_by_region[r.region] = segment_length / active_only_concurrency_count

                # credit this system in this region in the messy dictionary
                # of dictionaries, but skip devel system entries
                if r.system.active_or_preview():
                    if h.systemname not in t.system_region_mileages:
                        t.system_region_mileages[h.systemname] = dict()
                    t_system_dict = t.system_region_mileages[h.systemname]
                    if r.region in t_system_dict:
                        t_system_dict[r.region] = t_system_dict[r.region] + \
                                                  segment_length / system_concurrency_count
                    else:
                        t_system_dict[r.region] = segment_length / system_concurrency_count
print("!", flush=True)

print(et.et() + "Writing highway data stats log file (highwaydatastats.log).", flush=True)
hdstatsfile = open(args.logfilepath + "/highwaydatastats.log", "wt", encoding='UTF-8')
hdstatsfile.write("Travel Mapping highway mileage as of " + str(datetime.datetime.now()) + '\n')
active_only_miles = math.fsum(list(active_only_mileage_by_region.values()))
hdstatsfile.write("Active routes (active): " + "{0:.2f}".format(active_only_miles) + " mi\n")
active_preview_miles = math.fsum(list(active_preview_mileage_by_region.values()))
hdstatsfile.write("Clinchable routes (active, preview): " + "{0:.2f}".format(active_preview_miles) + " mi\n")
overall_miles = math.fsum(list(overall_mileage_by_region.values()))
hdstatsfile.write("All routes (active, preview, devel): " + "{0:.2f}".format(overall_miles) + " mi\n")
hdstatsfile.write("Breakdown by region:\n")
# let's sort alphabetically by region instead of using whatever order
# comes out of the dictionary
# a nice enhancement later here might break down by continent, then country,
# then region
region_entries = []
for region in list(overall_mileage_by_region.keys()):
    # look up active+preview and active-only mileages if they exist
    if region in list(active_preview_mileage_by_region.keys()):
        region_active_preview_miles = active_preview_mileage_by_region[region]
    else:
        region_active_preview_miles = 0.0
    if region in list(active_only_mileage_by_region.keys()):
        region_active_only_miles = active_only_mileage_by_region[region]
    else:
        region_active_only_miles = 0.0

    region_entries.append(region + ": " +
                          "{0:.2f}".format(region_active_only_miles) + " (active), " +
                          "{0:.2f}".format(region_active_preview_miles) + " (active, preview) " +
                          "{0:.2f}".format(overall_mileage_by_region[region]) + " (active, preview, devel)\n")
region_entries.sort()
for e in region_entries:
    hdstatsfile.write(e)

for h in highway_systems:
    hdstatsfile.write("System " + h.systemname + " (" + h.level + ") total: "
                      + "{0:.2f}".format(math.fsum(list(h.mileage_by_region.values()))) \
                      + ' mi\n')
    if len(h.mileage_by_region) > 1:
        hdstatsfile.write("System " + h.systemname + " by region:\n")
        for region in sorted(h.mileage_by_region.keys()):
            hdstatsfile.write(region + ": " + "{0:.2f}".format(h.mileage_by_region[region]) + " mi\n")
    hdstatsfile.write("System " + h.systemname + " by route:\n")
    for cr in h.con_route_list:
        con_total_miles = 0.0
        to_write = ""
        for r in cr.roots:
            to_write += "  " + r.readable_name() + ": " + "{0:.2f}".format(r.mileage) + " mi\n"
            con_total_miles += r.mileage
        cr.mileage = con_total_miles
        hdstatsfile.write(cr.readable_name() + ": " + "{0:.2f}".format(con_total_miles) + " mi")
        if len(cr.roots) == 1:
            hdstatsfile.write(" (" + cr.roots[0].readable_name() + " only)\n")
        else:
            hdstatsfile.write("\n" + to_write)

hdstatsfile.close()
# this will be used to store DB entry lines for clinchedSystemMileageByRegion
# table as needed values are computed here, to be added into the DB
# later in the program
csmbr_values = []
# and similar for DB entry lines for clinchedConnectedRoutes table
# and clinchedRoutes table
ccr_values = []
cr_values = []
# now add user clinched stats to their log entries
print(et.et() + "Creating per-traveler stats log entries and augmenting data structure.", end="", flush=True)
for t in traveler_lists:
    print(".", end="", flush=True)
    t.log_entries.append("Clinched Highway Statistics")
    t_active_only_miles = math.fsum(list(t.active_only_mileage_by_region.values()))
    t.log_entries.append("Overall in active systems: " + format_clinched_mi(t_active_only_miles, active_only_miles))
    t_active_preview_miles = math.fsum(list(t.active_preview_mileage_by_region.values()))
    t.log_entries.append(
        "Overall in active+preview systems: " + format_clinched_mi(t_active_preview_miles, active_preview_miles))

    t.log_entries.append("Overall by region: (each line reports active only then active+preview)")
    for region in sorted(t.active_preview_mileage_by_region.keys()):
        t_active_miles = 0.0
        total_active_miles = 0.0
        if region in list(t.active_only_mileage_by_region.keys()):
            t_active_miles = t.active_only_mileage_by_region[region]
            total_active_miles = active_only_mileage_by_region[region]
        t.log_entries.append(region + ": " +
                             format_clinched_mi(t_active_miles, total_active_miles) +
                             ", " +
                             format_clinched_mi(t.active_preview_mileage_by_region[region],
                                                active_preview_mileage_by_region[region]))

    t.active_systems_traveled = 0
    t.active_systems_clinched = 0
    t.preview_systems_traveled = 0
    t.preview_systems_clinched = 0
    active_systems = 0
    preview_systems = 0
    # "traveled" dictionaries indexed by system name, then conn or regular
    # route in another dictionary with keys route, values mileage
    # "clinched" dictionaries indexed by system name, values clinch count
    t.con_routes_traveled = dict()
    t.con_routes_clinched = dict()
    t.routes_traveled = dict()
    # t.routes_clinched = dict()

    # present stats by system here, also generate entries for
    # DB table clinchedSystemMileageByRegion as we compute and
    # have the data handy
    for h in highway_systems:
        if h.active_or_preview():
            if h.active():
                active_systems += 1
            else:
                preview_systems += 1
            t_system_overall = 0.0
            if h.systemname in t.system_region_mileages:
                t_system_overall = math.fsum(list(t.system_region_mileages[h.systemname].values()))
            t.log_entries.append("System " + h.systemname + " (" + h.level +
                                 ") overall: " +
                                 format_clinched_mi(t_system_overall, math.fsum(list(h.mileage_by_region.values()))))
            if t_system_overall > 0.0:
                if h.active():
                    t.active_systems_traveled += 1
                else:
                    t.preview_systems_traveled += 1
            if t_system_overall == math.fsum(list(h.mileage_by_region.values())):
                if h.active():
                    t.active_systems_clinched += 1
                else:
                    t.preview_systems_clinched += 1

            # stats by region covered by system, always in csmbr for
            # the DB, but add to logs only if it's been traveled at
            # all and it covers multiple regions
            if t_system_overall > 0.0:
                if len(h.mileage_by_region) > 1:
                    t.log_entries.append("System " + h.systemname + " by region:")
                for region in sorted(h.mileage_by_region.keys()):
                    system_region_mileage = 0.0
                    if h.systemname in t.system_region_mileages and region in t.system_region_mileages[h.systemname]:
                        system_region_mileage = t.system_region_mileages[h.systemname][region]
                        csmbr_values.append("('" + h.systemname + "','" + region + "','"
                                            + t.traveler_name + "','" +
                                            str(system_region_mileage) + "')")
                    if len(h.mileage_by_region) > 1:
                        t.log_entries.append("  " + region + ": " + \
                                             format_clinched_mi(system_region_mileage, h.mileage_by_region[region]))

            # stats by highway for the system, by connected route and
            # by each segment crossing region boundaries if applicable
            if t_system_overall > 0.0:
                system_con_dict = dict()
                t.con_routes_traveled[h.systemname] = system_con_dict
                con_routes_clinched = 0
                t.log_entries.append("System " + h.systemname + " by route (traveled routes only):")
                for cr in h.con_route_list:
                    con_total_miles = 0.0
                    con_clinched_miles = 0.0
                    to_write = ""
                    for r in cr.roots:
                        # find traveled mileage on this by this user
                        miles = r.clinched_by_traveler(t)
                        if miles > 0.0:
                            if miles >= r.mileage:
                                clinched = '1'
                            else:
                                clinched = '0'
                            cr_values.append("('" + r.root + "','" + t.traveler_name + "','" +
                                             str(miles) + "','" + clinched + "')")
                            t.routes_traveled[r] = miles
                            con_clinched_miles += miles
                            to_write += "  " + r.readable_name() + ": " + \
                                        format_clinched_mi(miles, r.mileage) + "\n"
                        con_total_miles += r.mileage
                    if con_clinched_miles > 0:
                        system_con_dict[cr] = con_clinched_miles
                        clinched = '0'
                        if con_clinched_miles == con_total_miles:
                            con_routes_clinched += 1
                            clinched = '1'
                        ccr_values.append("('" + cr.roots[0].root + "','" + t.traveler_name
                                          + "','" + str(con_clinched_miles) + "','"
                                          + clinched + "')")
                        t.log_entries.append(cr.readable_name() + ": " + \
                                             format_clinched_mi(con_clinched_miles, con_total_miles))
                        if len(cr.roots) == 1:
                            t.log_entries.append(" (" + cr.roots[0].readable_name() + " only)")
                        else:
                            t.log_entries.append(to_write)
                t.con_routes_clinched[h.systemname] = con_routes_clinched
                t.log_entries.append("System " + h.systemname + " connected routes traveled: " + \
                                     str(len(system_con_dict)) + " of " + \
                                     str(len(h.con_route_list)) + \
                                     " ({0:.1f}%)".format(100 * len(system_con_dict) / len(h.con_route_list)) + \
                                     ", clinched: " + str(con_routes_clinched) + " of " + \
                                     str(len(h.con_route_list)) + \
                                     " ({0:.1f}%)".format(100 * con_routes_clinched / len(h.con_route_list)) + \
                                     ".")

    # grand summary, active only
    t.log_entries.append("Traveled " + str(t.active_systems_traveled) + " of " + str(active_systems) +
                         " ({0:.1f}%)".format(100 * t.active_systems_traveled / active_systems) +
                         ", Clinched " + str(t.active_systems_clinched) + " of " + str(active_systems) +
                         " ({0:.1f}%)".format(100 * t.active_systems_clinched / active_systems) +
                         " active systems")
    # grand summary, active+preview
    t.log_entries.append("Traveled " + str(t.preview_systems_traveled) + " of " + str(preview_systems) +
                         " ({0:.1f}%)".format(100 * t.preview_systems_traveled / preview_systems) +
                         ", Clinched " + str(t.preview_systems_clinched) + " of " + str(preview_systems) +
                         " ({0:.1f}%)".format(100 * t.preview_systems_clinched / preview_systems) +
                         " preview systems")
print("!", flush=True)

# write log files for traveler lists
print(et.et() + "Writing traveler list logs.", flush=True)
for t in traveler_lists:
    t.write_log(args.logfilepath + "/users")

# write stats csv files
print(et.et() + "Writing stats csv files.", flush=True)

# first, overall per traveler by region, both active only and active+preview
allfile = open(args.csvstatfilepath + "/allbyregionactiveonly.csv", "w", encoding='UTF-8')
allfile.write("Traveler,Total")
regions = sorted(active_only_mileage_by_region.keys())
for region in regions:
    allfile.write(',' + region)
allfile.write('\n')
for t in traveler_lists:
    allfile.write(t.traveler_name + ",{0:.2f}".format(math.fsum(list(t.active_only_mileage_by_region.values()))))
    for region in regions:
        if region in t.active_only_mileage_by_region.keys():
            allfile.write(',{0:.2f}'.format(t.active_only_mileage_by_region[region]))
        else:
            allfile.write(',0')
    allfile.write('\n')
allfile.write('TOTAL,{0:.2f}'.format(math.fsum(list(active_only_mileage_by_region.values()))))
for region in regions:
    allfile.write(',{0:.2f}'.format(active_only_mileage_by_region[region]))
allfile.write('\n')
allfile.close()

# active+preview
allfile = open(args.csvstatfilepath + "/allbyregionactivepreview.csv", "w", encoding='UTF-8')
allfile.write("Traveler,Total")
regions = sorted(active_preview_mileage_by_region.keys())
for region in regions:
    allfile.write(',' + region)
allfile.write('\n')
for t in traveler_lists:
    allfile.write(t.traveler_name + ",{0:.2f}".format(math.fsum(list(t.active_preview_mileage_by_region.values()))))
    for region in regions:
        if region in t.active_preview_mileage_by_region.keys():
            allfile.write(',{0:.2f}'.format(t.active_preview_mileage_by_region[region]))
        else:
            allfile.write(',0')
    allfile.write('\n')
allfile.write('TOTAL,{0:.2f}'.format(math.fsum(list(active_preview_mileage_by_region.values()))))
for region in regions:
    allfile.write(',{0:.2f}'.format(active_preview_mileage_by_region[region]))
allfile.write('\n')
allfile.close()

# now, a file for each system, again per traveler by region
for h in highway_systems:
    sysfile = open(args.csvstatfilepath + "/" + h.systemname + '-all.csv', "w", encoding='UTF-8')
    sysfile.write('Traveler,Total')
    regions = sorted(h.mileage_by_region.keys())
    for region in regions:
        sysfile.write(',' + region)
    sysfile.write('\n')
    for t in traveler_lists:
        # only include entries for travelers who have any mileage in system
        if h.systemname in t.system_region_mileages:
            sysfile.write(
                t.traveler_name + ",{0:.2f}".format(math.fsum(list(t.system_region_mileages[h.systemname].values()))))
            for region in regions:
                if region in t.system_region_mileages[h.systemname]:
                    sysfile.write(',{0:.2f}'.format(t.system_region_mileages[h.systemname][region]))
                else:
                    sysfile.write(',0')
            sysfile.write('\n')
    sysfile.write('TOTAL,{0:.2f}'.format(math.fsum(list(h.mileage_by_region.values()))))
    for region in regions:
        sysfile.write(',{0:.2f}'.format(h.mileage_by_region[region]))
    sysfile.write('\n')
    sysfile.close()

# read in the datacheck false positives list
print(et.et() + "Reading datacheckfps.csv.", flush=True)
with open(args.highwaydatapath + "/datacheckfps.csv", "rt", encoding='utf-8') as file:
    lines = file.readlines()
file.close()

lines.pop(0)  # ignore header line
datacheckfps = []
datacheck_always_error = ['DUPLICATE_LABEL', 'HIDDEN_TERMINUS',
                          'LABEL_INVALID_CHAR', 'LABEL_SLASHES',
                          'LONG_UNDERSCORE', 'MALFORMED_URL',
                          'NONTERMINAL_UNDERSCORE']
for line in lines:
    fields = line.rstrip('\n').split(';')
    if len(fields) != 6:
        el.add_error("Could not parse datacheckfps.csv line: " + line)
        continue
    if fields[4] in datacheck_always_error:
        print("datacheckfps.csv line not allowed (always error): " + line)
        continue
    datacheckfps.append(fields)

# See if we have any errors that should be fatal to the site update process
if len(el.error_list) > 0:
    print("ABORTING due to " + str(len(el.error_list)) + " errors:")
    for i in range(len(el.error_list)):
        print(str(i + 1) + ": " + el.error_list[i])
    sys.exit(1)

# Build a graph structure out of all highway data in active and
# preview systems
print(et.et() + "Setting up for graphs of highway data.", flush=True)
graph_data = HighwayGraph(all_waypoints, highway_systems, datacheckerrors)

print(et.et() + "Writing graph waypoint simplification log.", flush=True)
logfile = open(args.logfilepath + '/waypointsimplification.log', 'w')
for line in graph_data.waypoint_naming_log:
    logfile.write(line + '\n')
logfile.close()
graph_data.waypoint_naming_log = None

# create list of graph information for the DB
graph_list = []
graph_types = []

# start generating graphs and making entries for graph DB table

if args.skipgraphs or args.errorcheck:
    print(et.et() + "SKIPPING generation of subgraphs.", flush=True)
else:
    print(et.et() + "Writing master TM simple graph file, tm-master-simple.tmg", flush=True)
    (sv, se) = graph_data.write_master_tmg_simple(args.graphfilepath + '/tm-master-simple.tmg')
    graph_list.append(GraphListEntry('tm-master-simple.tmg', 'All Travel Mapping Data', sv, se, 'simple', 'master'))
    print(et.et() + "Writing master TM collapsed graph file, tm-master.tmg.", flush=True)
    (cv, ce) = graph_data.write_master_tmg_collapsed(args.graphfilepath + '/tm-master.tmg')
    graph_list.append(GraphListEntry('tm-master.tmg', 'All Travel Mapping Data', cv, ce, 'collapsed', 'master'))
    graph_types.append(['master', 'All Travel Mapping Data',
                        'These graphs contain all routes currently plotted in the Travel Mapping project.'])

    # graphs restricted by place/area - from areagraphs.csv file
    print("\n" + et.et() + "Creating area data graphs.", flush=True)
    with open(args.highwaydatapath + "/graphs/areagraphs.csv", "rt", encoding='utf-8') as file:
        lines = file.readlines()
    file.close()
    lines.pop(0);  # ignore header line
    area_list = []
    for line in lines:
        fields = line.rstrip('\n').split(";")
        if len(fields) != 5:
            print("Could not parse areagraphs.csv line: " + line)
            continue
        area_list.append(PlaceRadius(*fields))

    for a in area_list:
        print(a.base + '(' + str(a.r) + ') ', end="", flush=True)
        graph_data.write_subgraphs_tmg(graph_list, args.graphfilepath + "/", a.base + str(a.r) + "-area",
                                       a.place + " (" + str(a.r) + " mi radius)", "area", None, None, a)
    graph_types.append(['area', 'Routes Within a Given Radius of a Place',
                        'These graphs contain all routes currently plotted within the given distance radius of the given place.'])
    print("!")

    # Graphs restricted by region
    print(et.et() + "Creating regional data graphs.", flush=True)

    # We will create graph data and a graph file for each region that includes
    # any active or preview systems
    for r in all_regions:
        region_code = r[0]
        if region_code not in active_preview_mileage_by_region:
            continue
        region_name = r[1]
        region_type = r[4]
        print(region_code + ' ', end="", flush=True)
        graph_data.write_subgraphs_tmg(graph_list, args.graphfilepath + "/", region_code + "-region",
                                       region_name + " (" + region_type + ")", "region", [region_code], None, None)
    graph_types.append(['region', 'Routes Within a Single Region',
                        'These graphs contain all routes currently plotted within the given region.'])
    print("!")

    # Graphs restricted by system - from systemgraphs.csv file
    print(et.et() + "Creating system data graphs.", flush=True)

    # We will create graph data and a graph file for only a few interesting
    # systems, as many are not useful on their own
    h = None
    with open(args.highwaydatapath + "/graphs/systemgraphs.csv", "rt", encoding='utf-8') as file:
        lines = file.readlines()
    file.close()
    lines.pop(0);  # ignore header line
    for hname in lines:
        h = None
        for hs in highway_systems:
            if hs.systemname == hname.strip():
                h = hs
                break
        if h is not None:
            print(h.systemname + ' ', end="", flush=True)
            graph_data.write_subgraphs_tmg(graph_list, args.graphfilepath + "/", h.systemname + "-system",
                                           h.systemname + " (" + h.fullname + ")", "system", None, [h], None)
    if h is not None:
        graph_types.append(['system', 'Routes Within a Single Highway System',
                            'These graphs contain the routes within a single highway system and are not restricted by region.'])
    print("!")

    # Some additional interesting graphs, the "multisystem" graphs
    print(et.et() + "Creating multisystem graphs.", flush=True)

    with open(args.highwaydatapath + "/graphs/multisystem.csv", "rt", encoding='utf-8') as file:
        lines = file.readlines()
    file.close()
    lines.pop(0);  # ignore header line
    for line in lines:
        fields = line.rstrip('\n').split(";")
        if len(fields) != 3:
            print("Could not parse multisystem.csv line: " + line)
            continue
        print(fields[1] + ' ', end="", flush=True)
        systems = []
        selected_systems = fields[2].split(",")
        for h in highway_systems:
            if h.systemname in selected_systems:
                systems.append(h)
        graph_data.write_subgraphs_tmg(graph_list, args.graphfilepath + "/", fields[1],
                                       fields[0], "multisystem", None, systems, None)
    graph_types.append(['multisystem', 'Routes Within Multiple Highway Systems',
                        'These graphs contain the routes within a set of highway systems.'])
    print("!")

    # Some additional interesting graphs, the "multiregion" graphs
    print(et.et() + "Creating multiregion graphs.", flush=True)

    with open(args.highwaydatapath + "/graphs/multiregion.csv", "rt", encoding='utf-8') as file:
        lines = file.readlines()
    file.close()
    lines.pop(0);  # ignore header line
    for line in lines:
        fields = line.rstrip('\n').split(";")
        if len(fields) != 3:
            print("Could not parse multiregion.csv line: " + line)
            continue
        print(fields[1] + ' ', end="", flush=True)
        region_list = []
        selected_regions = fields[2].split(",")
        for r in all_regions:
            if r[0] in selected_regions and r[0] in active_preview_mileage_by_region:
                region_list.append(r[0])
        graph_data.write_subgraphs_tmg(graph_list, args.graphfilepath + "/", fields[1],
                                       fields[0], "multiregion", region_list, None, None)
    graph_types.append(['multiregion', 'Routes Within Multiple Regions',
                        'These graphs contain the routes within a set of regions.'])
    print("!")

    # country graphs - we find countries that have regions
    # that have routes with active or preview mileage
    print(et.et() + "Creating country graphs.", flush=True)
    for c in countries:
        region_list = []
        for r in all_regions:
            # does it match this country and have routes?
            if c[0] == r[2] and r[0] in active_preview_mileage_by_region:
                region_list.append(r[0])
        # does it have at least two?  if none, no data, if 1 we already
        # generated a graph for that one region
        if len(region_list) >= 2:
            print(c[0] + " ", end="", flush=True)
            graph_data.write_subgraphs_tmg(graph_list, args.graphfilepath + "/", c[0] + "-country",
                                           c[1] + " All Routes in Country", "country", region_list, None, None)
    graph_types.append(['country', 'Routes Within a Single Multi-Region Country',
                        'These graphs contain the routes within a single country that is composed of multiple regions that contain plotted routes.  Countries consisting of a single region are represented by their regional graph.'])
    print("!")

    # continent graphs -- any continent with data will be created
    print(et.et() + "Creating continent graphs.", flush=True)
    for c in continents:
        region_list = []
        for r in all_regions:
            # does it match this continent and have routes?
            if c[0] == r[3] and r[0] in active_preview_mileage_by_region:
                region_list.append(r[0])
        # generate for any continent with at least 1 region with mileage
        if len(region_list) >= 1:
            print(c[0] + " ", end="", flush=True)
            graph_data.write_subgraphs_tmg(graph_list, args.graphfilepath + "/", c[0] + "-continent",
                                           c[1] + " All Routes on Continent", "continent", region_list, None, None)
    graph_types.append(['continent', 'Routes Within a Continent',
                        'These graphs contain the routes on a continent.'])
    print("!")

# data check: visit each system and route and check for various problems
print(et.et() + "Performing data checks.", end="", flush=True)
# perform most datachecks here (list initialized above)
for h in highway_systems:
    print(".", end="", flush=True)
    for r in h.route_list:
        # set to be used per-route to find label duplicates
        all_route_labels = set()
        # set of tuples to be used for finding duplicate coordinates
        coords_used = set()

        visible_distance = 0.0
        # note that we assume the first point will be visible in each route
        # so the following is simply a placeholder
        last_visible = None
        prev_w = None

        # look for hidden termini
        if r.point_list[0].is_hidden:
            datacheckerrors.append(DatacheckEntry(r, [r.point_list[0].label], 'HIDDEN_TERMINUS'))
        if r.point_list[len(r.point_list) - 1].is_hidden:
            datacheckerrors.append(DatacheckEntry(r, [r.point_list[len(r.point_list) - 1].label], 'HIDDEN_TERMINUS'))

        for w in r.point_list:
            # duplicate labels
            label_list = w.alt_labels.copy()
            label_list.append(w.label)
            for label in label_list:
                lower_label = label.lower().strip("+*")
                if lower_label in all_route_labels:
                    datacheckerrors.append(DatacheckEntry(r, [lower_label], "DUPLICATE_LABEL"))
                else:
                    all_route_labels.add(lower_label)

            # out-of-bounds coords
            if w.lat > 90 or w.lat < -90 or w.lng > 180 or w.lng < -180:
                datacheckerrors.append(DatacheckEntry(r, [w.label], 'OUT_OF_BOUNDS',
                                                      "(" + str(w.lat) + "," + str(w.lng) + ")"))

            # duplicate coordinates
            latlng = w.lat, w.lng
            if latlng in coords_used:
                for other_w in r.point_list:
                    if w == other_w:
                        break
                    if w.lat == other_w.lat and w.lng == other_w.lng and w.label != other_w.label:
                        labels = []
                        labels.append(other_w.label)
                        labels.append(w.label)
                        datacheckerrors.append(DatacheckEntry(r, labels, "DUPLICATE_COORDS",
                                                              "(" + str(latlng[0]) + "," + str(latlng[1]) + ")"))
            else:
                coords_used.add(latlng)

            # visible distance update, and last segment length check
            if prev_w is not None:
                last_distance = w.distance_to(prev_w)
                visible_distance += last_distance
                if last_distance > 20.0:
                    labels = []
                    labels.append(prev_w.label)
                    labels.append(w.label)
                    datacheckerrors.append(DatacheckEntry(r, labels, 'LONG_SEGMENT',
                                                          "{0:.2f}".format(last_distance)))

            if not w.is_hidden:
                # complete visible distance check, omit report for active
                # systems to reduce clutter
                if visible_distance > 10.0 and not h.active():
                    labels = []
                    labels.append(last_visible.label)
                    labels.append(w.label)
                    datacheckerrors.append(DatacheckEntry(r, labels, 'VISIBLE_DISTANCE',
                                                          "{0:.2f}".format(visible_distance)))
                last_visible = w
                visible_distance = 0.0

                # looking for the route within the label
                # match_start = w.label.find(r.route)
                # if match_start >= 0:
                # we have a potential match, just need to make sure if the route
                # name ends with a number that the matched substring isn't followed
                # by more numbers (e.g., NY50 is an OK label in NY5)
                #    if len(r.route) + match_start == len(w.label) or \
                #            not w.label[len(r.route) + match_start].isdigit():
                # partially complete "references own route" -- too many FP
                # or re.fullmatch('.*/'+r.route+'.*',w.label[w.label) :
                # first check for number match after a slash, if there is one
                selfref_found = False
                if '/' in w.label and r.route[-1].isdigit():
                    digit_starts = len(r.route) - 1
                    while digit_starts >= 0 and r.route[digit_starts].isdigit():
                        digit_starts -= 1
                    if w.label[w.label.index('/') + 1:] == r.route[digit_starts + 1:]:
                        selfref_found = True
                    if w.label[w.label.index('/') + 1:] == r.route:
                        selfref_found = True
                    if '_' in w.label[w.label.index('/') + 1:] and w.label[w.label.index('/') + 1:w.label.rindex(
                            '_')] == r.route[digit_starts + 1:]:
                        selfref_found = True
                    if '_' in w.label[w.label.index('/') + 1:] and w.label[w.label.index('/') + 1:w.label.rindex(
                            '_')] == r.route:
                        selfref_found = True

                # now the remaining checks
                if selfref_found or r.route + r.banner == w.label or re.fullmatch(r.route + r.banner + '[_/].*',
                                                                                  w.label):
                    datacheckerrors.append(DatacheckEntry(r, [w.label], 'LABEL_SELFREF'))

                # look for too many underscores in label
                if w.label.count('_') > 1:
                    datacheckerrors.append(DatacheckEntry(r, [w.label], 'LABEL_UNDERSCORES'))

                # look for too many characters after underscore in label
                if '_' in w.label:
                    if w.label.index('_') < len(w.label) - 5:
                        datacheckerrors.append(DatacheckEntry(r, [w.label], 'LONG_UNDERSCORE'))

                # look for too many slashes in label
                if w.label.count('/') > 1:
                    datacheckerrors.append(DatacheckEntry(r, [w.label], 'LABEL_SLASHES'))

                # look for parenthesis balance in label
                if w.label.count('(') != w.label.count(')'):
                    datacheckerrors.append(DatacheckEntry(r, [w.label], 'LABEL_PARENS'))

                # look for labels with invalid characters
                if not re.fullmatch('[a-zA-Z0-9()/\+\*_\-\.]+', w.label):
                    datacheckerrors.append(DatacheckEntry(r, [w.label], 'LABEL_INVALID_CHAR'))
                for a in w.alt_labels:
                    if not re.fullmatch('[a-zA-Z0-9()/\+\*_\-\.]+', a):
                        datacheckerrors.append(DatacheckEntry(r, [a], 'LABEL_INVALID_CHAR'))

                # look for labels with a slash after an underscore
                if '_' in w.label and '/' in w.label and \
                        w.label.index('/') > w.label.index('_'):
                    datacheckerrors.append(DatacheckEntry(r, [w.label], 'NONTERMINAL_UNDERSCORE'))

                # look for I-xx with Bus instead of BL or BS
                if re.fullmatch('I\-[0-9]*Bus', w.label):
                    datacheckerrors.append(DatacheckEntry(r, [w.label], 'BUS_WITH_I'))

                # look for labels that look like hidden waypoints but
                # which aren't hidden
                if re.fullmatch('X[0-9][0-9][0-9][0-9][0-9][0-9]', w.label):
                    datacheckerrors.append(DatacheckEntry(r, [w.label], 'LABEL_LOOKS_HIDDEN'))

                # look for USxxxA but not USxxxAlt, B/Bus (others?)
                ##if re.fullmatch('US[0-9]+A.*', w.label) and not re.fullmatch('US[0-9]+Alt.*', w.label) or \
                ##   re.fullmatch('US[0-9]+B.*', w.label) and \
                ##   not (re.fullmatch('US[0-9]+Bus.*', w.label) or re.fullmatch('US[0-9]+Byp.*', w.label)):
                ##    datacheckerrors.append(DatacheckEntry(r,[w.label],'US_BANNER'))

            prev_w = w

        # angle check is easier with a traditional for loop and array indices
        for i in range(1, len(r.point_list) - 1):
            # print("computing angle for " + str(r.point_list[i-1]) + ' ' + str(r.point_list[i]) + ' ' + str(r.point_list[i+1]))
            if r.point_list[i - 1].same_coords(r.point_list[i]) or \
                    r.point_list[i + 1].same_coords(r.point_list[i]):
                labels = []
                labels.append(r.point_list[i - 1].label)
                labels.append(r.point_list[i].label)
                labels.append(r.point_list[i + 1].label)
                datacheckerrors.append(DatacheckEntry(r, labels, 'BAD_ANGLE'))
            else:
                angle = r.point_list[i].angle(r.point_list[i - 1], r.point_list[i + 1])
                if angle > 135:
                    labels = []
                    labels.append(r.point_list[i - 1].label)
                    labels.append(r.point_list[i].label)
                    labels.append(r.point_list[i + 1].label)
                    datacheckerrors.append(DatacheckEntry(r, labels, 'SHARP_ANGLE',
                                                          "{0:.2f}".format(angle)))
print("!", flush=True)
print(et.et() + "Found " + str(len(datacheckerrors)) + " datacheck errors.")

datacheckerrors.sort(key=lambda DatacheckEntry: str(DatacheckEntry))

# now mark false positives
print(et.et() + "Marking datacheck false positives.", end="", flush=True)
fpfile = open(args.logfilepath + '/nearmatchfps.log', 'w', encoding='utf-8')
fpfile.write("Log file created at: " + str(datetime.datetime.now()) + "\n")
toremove = []
counter = 0
fpcount = 0
for d in datacheckerrors:
    # print("Checking: " + str(d))
    counter += 1
    if counter % 1000 == 0:
        print(".", end="", flush=True)
    for fp in datacheckfps:
        # print("Comparing: " + str(d) + " to " + str(fp))
        if d.match_except_info(fp):
            if d.info == fp[5]:
                # print("Match!")
                d.fp = True
                fpcount += 1
                datacheckfps.remove(fp)
                break
            fpfile.write(
                "FP_ENTRY: " + fp[0] + ';' + fp[1] + ';' + fp[2] + ';' + fp[3] + ';' + fp[4] + ';' + fp[5] + '\n')
            fpfile.write(
                "CHANGETO: " + fp[0] + ';' + fp[1] + ';' + fp[2] + ';' + fp[3] + ';' + fp[4] + ';' + d.info + '\n')
fpfile.close()
print("!", flush=True)
print(et.et() + "Matched " + str(fpcount) + " FP entries.", flush=True)

# write log of unmatched false positives from the datacheckfps.csv
print(et.et() + "Writing log of unmatched datacheck FP entries.")
fpfile = open(args.logfilepath + '/unmatchedfps.log', 'w', encoding='utf-8')
fpfile.write("Log file created at: " + str(datetime.datetime.now()) + "\n")
if len(datacheckfps) > 0:
    for entry in datacheckfps:
        fpfile.write(
            entry[0] + ';' + entry[1] + ';' + entry[2] + ';' + entry[3] + ';' + entry[4] + ';' + entry[5] + '\n')
else:
    fpfile.write("No unmatched FP entries.")
fpfile.close()

# datacheck.log file
print(et.et() + "Writing datacheck.log")
logfile = open(args.logfilepath + '/datacheck.log', 'w')
logfile.write("Log file created at: " + str(datetime.datetime.now()) + "\n")
logfile.write("Datacheck errors that have been flagged as false positives are not included.\n")
logfile.write("These entries should be in a format ready to paste into datacheckfps.csv.\n")
logfile.write("Root;Waypoint1;Waypoint2;Waypoint3;Error;Info\n")
if len(datacheckerrors) > 0:
    for d in datacheckerrors:
        if not d.fp:
            logfile.write(str(d) + "\n")
else:
    logfile.write("No datacheck errors found.")
logfile.close()

if args.errorcheck:
    print(et.et() + "SKIPPING database file.")
else:
    print(et.et() + "Writing database file " + args.databasename + ".sql.")
    # Once all data is read in and processed, create a .sql file that will
    # create all of the DB tables to be used by other parts of the project
    sqlfile = open(args.databasename + '.sql', 'w', encoding='UTF-8')
    # Note: removed "USE" line, DB name must be specified on the mysql command line

    # we have to drop tables in the right order to avoid foreign key errors
    sqlfile.write('DROP TABLE IF EXISTS datacheckErrors;\n')
    sqlfile.write('DROP TABLE IF EXISTS clinchedConnectedRoutes;\n')
    sqlfile.write('DROP TABLE IF EXISTS clinchedRoutes;\n')
    sqlfile.write('DROP TABLE IF EXISTS clinchedOverallMileageByRegion;\n')
    sqlfile.write('DROP TABLE IF EXISTS clinchedSystemMileageByRegion;\n')
    sqlfile.write('DROP TABLE IF EXISTS overallMileageByRegion;\n')
    sqlfile.write('DROP TABLE IF EXISTS systemMileageByRegion;\n')
    sqlfile.write('DROP TABLE IF EXISTS clinched;\n')
    sqlfile.write('DROP TABLE IF EXISTS segments;\n')
    sqlfile.write('DROP TABLE IF EXISTS waypoints;\n')
    sqlfile.write('DROP TABLE IF EXISTS connectedRouteRoots;\n')
    sqlfile.write('DROP TABLE IF EXISTS connectedRoutes;\n')
    sqlfile.write('DROP TABLE IF EXISTS routes;\n')
    sqlfile.write('DROP TABLE IF EXISTS systems;\n')
    sqlfile.write('DROP TABLE IF EXISTS updates;\n')
    sqlfile.write('DROP TABLE IF EXISTS systemUpdates;\n')
    sqlfile.write('DROP TABLE IF EXISTS regions;\n')
    sqlfile.write('DROP TABLE IF EXISTS countries;\n')
    sqlfile.write('DROP TABLE IF EXISTS continents;\n')

    # first, continents, countries, and regions
    sqlfile.write('CREATE TABLE continents (code VARCHAR(3), name VARCHAR(15), PRIMARY KEY(code));\n')
    sqlfile.write('INSERT INTO continents VALUES\n')
    first = True
    for c in continents:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write("('" + c[0] + "','" + c[1] + "')\n")
    sqlfile.write(";\n")

    sqlfile.write('CREATE TABLE countries (code VARCHAR(3), name VARCHAR(32), PRIMARY KEY(code));\n')
    sqlfile.write('INSERT INTO countries VALUES\n')
    first = True
    for c in countries:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write("('" + c[0] + "','" + c[1].replace("'", "''") + "')\n")
    sqlfile.write(";\n")

    sqlfile.write(
        'CREATE TABLE regions (code VARCHAR(8), name VARCHAR(48), country VARCHAR(3), continent VARCHAR(3), regiontype VARCHAR(32), PRIMARY KEY(code), FOREIGN KEY (country) REFERENCES countries(code), FOREIGN KEY (continent) REFERENCES continents(code));\n')
    sqlfile.write('INSERT INTO regions VALUES\n')
    first = True
    for r in all_regions:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write(
            "('" + r[0] + "','" + r[1].replace("'", "''") + "','" + r[2] + "','" + r[3] + "','" + r[4] + "')\n")
    sqlfile.write(";\n")

    # next, a table of the systems, consisting of the system name in the
    # field 'name', the system's country code, its full name, the default
    # color for its mapping, a level (one of active, preview, devel), and
    # a boolean indicating if the system is active for mapping in the
    # project in the field 'active'
    sqlfile.write(
        'CREATE TABLE systems (systemName VARCHAR(10), countryCode CHAR(3), fullName VARCHAR(60), color VARCHAR(16), level VARCHAR(10), tier INTEGER, csvOrder INTEGER, PRIMARY KEY(systemName));\n')
    sqlfile.write('INSERT INTO systems VALUES\n')
    first = True
    csvOrder = 0
    for h in highway_systems:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write("('" + h.systemname + "','" + h.country + "','" +
                      h.fullname + "','" + h.color + "','" + h.level +
                      "','" + str(h.tier) + "','" + str(csvOrder) + "')\n")
        csvOrder += 1
    sqlfile.write(";\n")

    # next, a table of highways, with the same fields as in the first line
    sqlfile.write(
        'CREATE TABLE routes (systemName VARCHAR(10), region VARCHAR(8), route VARCHAR(16), banner VARCHAR(6), abbrev VARCHAR(3), city VARCHAR(100), root VARCHAR(32), mileage FLOAT, rootOrder INTEGER, csvOrder INTEGER, PRIMARY KEY(root), FOREIGN KEY (systemName) REFERENCES systems(systemName));\n')
    sqlfile.write('INSERT INTO routes VALUES\n')
    first = True
    csvOrder = 0
    for h in highway_systems:
        for r in h.route_list:
            if not first:
                sqlfile.write(",")
            first = False
            sqlfile.write("(" + r.csv_line() + ",'" + str(csvOrder) + "')\n")
            csvOrder += 1
    sqlfile.write(";\n")

    # connected routes table, but only first "root" in each in this table
    sqlfile.write(
        'CREATE TABLE connectedRoutes (systemName VARCHAR(10), route VARCHAR(16), banner VARCHAR(6), groupName VARCHAR(100), firstRoot VARCHAR(32), mileage FLOAT, csvOrder INTEGER, PRIMARY KEY(firstRoot), FOREIGN KEY (firstRoot) REFERENCES routes(root));\n')
    sqlfile.write('INSERT INTO connectedRoutes VALUES\n')
    first = True
    csvOrder = 0
    for h in highway_systems:
        for cr in h.con_route_list:
            if not first:
                sqlfile.write(",")
            first = False
            sqlfile.write("(" + cr.csv_line() + ",'" + str(csvOrder) + "')\n")
            csvOrder += 1
    sqlfile.write(";\n")

    # This table has remaining roots for any connected route
    # that connects multiple routes/roots
    sqlfile.write(
        'CREATE TABLE connectedRouteRoots (firstRoot VARCHAR(32), root VARCHAR(32), FOREIGN KEY (firstRoot) REFERENCES connectedRoutes(firstRoot));\n')
    first = True
    for h in highway_systems:
        for cr in h.con_route_list:
            if len(cr.roots) > 1:
                for i in range(1, len(cr.roots)):
                    if first:
                        sqlfile.write('INSERT INTO connectedRouteRoots VALUES\n')
                    if not first:
                        sqlfile.write(",")
                    first = False
                    sqlfile.write("('" + cr.roots[0].root + "','" + cr.roots[i].root + "')\n")
    sqlfile.write(";\n")

    # Now, a table with raw highway route data: list of points, in order, that define the route
    sqlfile.write(
        'CREATE TABLE waypoints (pointId INTEGER, pointName VARCHAR(20), latitude DOUBLE, longitude DOUBLE, root VARCHAR(32), PRIMARY KEY(pointId), FOREIGN KEY (root) REFERENCES routes(root));\n')
    point_num = 0
    for h in highway_systems:
        for r in h.route_list:
            sqlfile.write('INSERT INTO waypoints VALUES\n')
            first = True
            for w in r.point_list:
                if not first:
                    sqlfile.write(",")
                first = False
                w.point_num = point_num
                sqlfile.write("(" + w.csv_line(point_num) + ")\n")
                point_num += 1
            sqlfile.write(";\n")

    # Build indices to speed latitude/longitude joins for intersecting highway queries
    sqlfile.write('CREATE INDEX `latitude` ON waypoints(`latitude`);\n')
    sqlfile.write('CREATE INDEX `longitude` ON waypoints(`longitude`);\n')

    # Table of all HighwaySegments.
    sqlfile.write(
        'CREATE TABLE segments (segmentId INTEGER, waypoint1 INTEGER, waypoint2 INTEGER, root VARCHAR(32), PRIMARY KEY (segmentId), FOREIGN KEY (waypoint1) REFERENCES waypoints(pointId), FOREIGN KEY (waypoint2) REFERENCES waypoints(pointId), FOREIGN KEY (root) REFERENCES routes(root));\n')
    segment_num = 0
    clinched_list = []
    for h in highway_systems:
        for r in h.route_list:
            sqlfile.write('INSERT INTO segments VALUES\n')
            first = True
            for s in r.segment_list:
                if not first:
                    sqlfile.write(",")
                first = False
                sqlfile.write("(" + s.csv_line(segment_num) + ")\n")
                for t in s.clinched_by:
                    clinched_list.append("'" + str(segment_num) + "','" + t.traveler_name + "'")
                segment_num += 1
            sqlfile.write(";\n")

    # maybe a separate traveler table will make sense but for now, I'll just use
    # the name from the .list name
    sqlfile.write(
        'CREATE TABLE clinched (segmentId INTEGER, traveler VARCHAR(48), FOREIGN KEY (segmentId) REFERENCES segments(segmentId));\n')
    for start in range(0, len(clinched_list), 10000):
        sqlfile.write('INSERT INTO clinched VALUES\n')
        first = True
        for c in clinched_list[start:start + 10000]:
            if not first:
                sqlfile.write(",")
            first = False
            sqlfile.write("(" + c + ")\n")
        sqlfile.write(";\n")

    # overall mileage by region data (with concurrencies accounted for,
    # active systems only then active+preview)
    sqlfile.write(
        'CREATE TABLE overallMileageByRegion (region VARCHAR(8), activeMileage FLOAT, activePreviewMileage FLOAT);\n')
    sqlfile.write('INSERT INTO overallMileageByRegion VALUES\n')
    first = True
    for region in list(active_preview_mileage_by_region.keys()):
        if not first:
            sqlfile.write(",")
        first = False
        active_only_mileage = 0.0
        active_preview_mileage = 0.0
        if region in list(active_only_mileage_by_region.keys()):
            active_only_mileage = active_only_mileage_by_region[region]
        if region in list(active_preview_mileage_by_region.keys()):
            active_preview_mileage = active_preview_mileage_by_region[region]
        sqlfile.write("('" + region + "','" +
                      str(active_only_mileage) + "','" +
                      str(active_preview_mileage) + "')\n")
    sqlfile.write(";\n")

    # system mileage by region data (with concurrencies accounted for,
    # active systems and preview systems only)
    sqlfile.write(
        'CREATE TABLE systemMileageByRegion (systemName VARCHAR(10), region VARCHAR(8), mileage FLOAT, FOREIGN KEY (systemName) REFERENCES systems(systemName));\n')
    sqlfile.write('INSERT INTO systemMileageByRegion VALUES\n')
    first = True
    for h in highway_systems:
        if h.active_or_preview():
            for region in list(h.mileage_by_region.keys()):
                if not first:
                    sqlfile.write(",")
                first = False
                sqlfile.write("('" + h.systemname + "','" + region + "','" + str(h.mileage_by_region[region]) + "')\n")
    sqlfile.write(";\n")

    # clinched overall mileage by region data (with concurrencies
    # accounted for, active systems and preview systems only)
    sqlfile.write(
        'CREATE TABLE clinchedOverallMileageByRegion (region VARCHAR(8), traveler VARCHAR(48), activeMileage FLOAT, activePreviewMileage FLOAT);\n')
    sqlfile.write('INSERT INTO clinchedOverallMileageByRegion VALUES\n')
    first = True
    for t in traveler_lists:
        for region in list(t.active_preview_mileage_by_region.keys()):
            if not first:
                sqlfile.write(",")
            first = False
            active_miles = 0.0
            if region in list(t.active_only_mileage_by_region.keys()):
                active_miles = t.active_only_mileage_by_region[region]
            sqlfile.write("('" + region + "','" + t.traveler_name + "','" +
                          str(active_miles) + "','" +
                          str(t.active_preview_mileage_by_region[region]) + "')\n")
    sqlfile.write(";\n")

    # clinched system mileage by region data (with concurrencies accounted
    # for, active systems and preview systems only)
    sqlfile.write(
        'CREATE TABLE clinchedSystemMileageByRegion (systemName VARCHAR(10), region VARCHAR(8), traveler VARCHAR(48), mileage FLOAT, FOREIGN KEY (systemName) REFERENCES systems(systemName));\n')
    sqlfile.write('INSERT INTO clinchedSystemMileageByRegion VALUES\n')
    first = True
    for line in csmbr_values:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write(line + "\n")
    sqlfile.write(";\n")

    # clinched mileage by connected route, active systems and preview
    # systems only
    sqlfile.write(
        'CREATE TABLE clinchedConnectedRoutes (route VARCHAR(32), traveler VARCHAR(48), mileage FLOAT, clinched BOOLEAN, FOREIGN KEY (route) REFERENCES connectedRoutes(firstRoot));\n')
    for start in range(0, len(ccr_values), 10000):
        sqlfile.write('INSERT INTO clinchedConnectedRoutes VALUES\n')
        first = True
        for line in ccr_values[start:start + 10000]:
            if not first:
                sqlfile.write(",")
            first = False
            sqlfile.write(line + "\n")
        sqlfile.write(";\n")

    # clinched mileage by route, active systems and preview systems only
    sqlfile.write(
        'CREATE TABLE clinchedRoutes (route VARCHAR(32), traveler VARCHAR(48), mileage FLOAT, clinched BOOLEAN, FOREIGN KEY (route) REFERENCES routes(root));\n')
    for start in range(0, len(cr_values), 10000):
        sqlfile.write('INSERT INTO clinchedRoutes VALUES\n')
        first = True
        for line in cr_values[start:start + 10000]:
            if not first:
                sqlfile.write(",")
            first = False
            sqlfile.write(line + "\n")
        sqlfile.write(";\n")

    # updates entries
    sqlfile.write(
        'CREATE TABLE updates (date VARCHAR(10), region VARCHAR(60), route VARCHAR(80), root VARCHAR(32), description VARCHAR(1024));\n')
    sqlfile.write('INSERT INTO updates VALUES\n')
    first = True
    for update in updates:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write(
            "('" + update[0] + "','" + update[1].replace("'", "''") + "','" + update[2].replace("'", "''") + "','" +
            update[3] + "','" + update[4].replace("'", "''") + "')\n")
    sqlfile.write(";\n")

    # systemUpdates entries
    sqlfile.write(
        'CREATE TABLE systemUpdates (date VARCHAR(10), region VARCHAR(48), systemName VARCHAR(10), description VARCHAR(128), statusChange VARCHAR(16));\n')
    sqlfile.write('INSERT INTO systemUpdates VALUES\n')
    first = True
    for systemupdate in systemupdates:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write(
            "('" + systemupdate[0] + "','" + systemupdate[1].replace("'", "''") + "','" + systemupdate[2] + "','" +
            systemupdate[3].replace("'", "''") + "','" + systemupdate[4] + "')\n")
    sqlfile.write(";\n")

    # datacheck errors into the db
    sqlfile.write(
        'CREATE TABLE datacheckErrors (route VARCHAR(32), label1 VARCHAR(50), label2 VARCHAR(20), label3 VARCHAR(20), code VARCHAR(20), value VARCHAR(32), falsePositive BOOLEAN, FOREIGN KEY (route) REFERENCES routes(root));\n')
    if len(datacheckerrors) > 0:
        sqlfile.write('INSERT INTO datacheckErrors VALUES\n')
        first = True
        for d in datacheckerrors:
            if not first:
                sqlfile.write(',')
            first = False
            sqlfile.write("('" + str(d.route.root) + "',")
            if len(d.labels) == 0:
                sqlfile.write("'','','',")
            elif len(d.labels) == 1:
                sqlfile.write("'" + d.labels[0] + "','','',")
            elif len(d.labels) == 2:
                sqlfile.write("'" + d.labels[0] + "','" + d.labels[1] + "','',")
            else:
                sqlfile.write("'" + d.labels[0] + "','" + d.labels[1] + "','" + d.labels[2] + "',")
            if d.fp:
                fp = '1'
            else:
                fp = '0'
            sqlfile.write("'" + d.code + "','" + d.info + "','" + fp + "')\n")
    sqlfile.write(";\n")

    # update graph info in DB if graphs were generated
    if not args.skipgraphs:
        sqlfile.write('DROP TABLE IF EXISTS graphs;\n')
        sqlfile.write('DROP TABLE IF EXISTS graphTypes;\n')
        sqlfile.write(
            'CREATE TABLE graphTypes (category VARCHAR(12), descr VARCHAR(100), longDescr TEXT, PRIMARY KEY(category));\n')
        if len(graph_types) > 0:
            sqlfile.write('INSERT INTO graphTypes VALUES\n')
            first = True
            for g in graph_types:
                if not first:
                    sqlfile.write(',')
                first = False
                sqlfile.write("('" + g[0] + "','" + g[1] + "','" + g[2] + "')\n")
            sqlfile.write(";\n")

        sqlfile.write(
            'CREATE TABLE graphs (filename VARCHAR(32), descr VARCHAR(100), vertices INTEGER, edges INTEGER, format VARCHAR(10), category VARCHAR(12), FOREIGN KEY (category) REFERENCES graphTypes(category));\n')
        if len(graph_list) > 0:
            sqlfile.write('INSERT INTO graphs VALUES\n')
            first = True
            for g in graph_list:
                if not first:
                    sqlfile.write(',')
                first = False
                sqlfile.write(
                    "('" + g.filename + "','" + g.descr.replace("'", "''") + "','" + str(g.vertices) + "','" + str(
                        g.edges) + "','" + g.format + "','" + g.category + "')\n")
            sqlfile.write(";\n")

    sqlfile.close()

# print some statistics
print(et.et() + "Processed " + str(len(highway_systems)) + " highway systems.")
routes = 0
points = 0
segments = 0
for h in highway_systems:
    routes += len(h.route_list)
    for r in h.route_list:
        points += len(r.point_list)
        segments += len(r.segment_list)
print("Processed " + str(routes) + " routes with a total of " + \
      str(points) + " points and " + str(segments) + " segments.")
if points != all_waypoints.size():
    print("MISMATCH: all_waypoints contains " + str(all_waypoints.size()) + " waypoints!")
print("WaypointQuadtree contains " + str(all_waypoints.total_nodes()) + " total nodes.")

if not args.errorcheck:
    # compute colocation of waypoints stats
    print(et.et() + "Computing waypoint colocation stats, reporting all with 8 or more colocations:")
    largest_colocate_count = all_waypoints.max_colocated()
    colocate_counts = [0] * (largest_colocate_count + 1)
    big_colocate_locations = dict()
    for w in all_waypoints.point_list():
        c = w.num_colocated()
        if c >= 8:
            point = (w.lat, w.lng)
            entry = w.route.root + " " + w.label
            if point in big_colocate_locations:
                the_list = big_colocate_locations[point]
                the_list.append(entry)
                big_colocate_locations[point] = the_list
            else:
                the_list = []
                the_list.append(entry)
                big_colocate_locations[point] = the_list
            # print(str(w) + " with " + str(c) + " other points.")
        colocate_counts[c] += 1
    for place in big_colocate_locations:
        the_list = big_colocate_locations[place]
        print(str(place) + " is occupied by " + str(len(the_list)) + " waypoints: " + str(the_list))
    print("Waypoint colocation counts:")
    unique_locations = 0
    for c in range(1, largest_colocate_count + 1):
        unique_locations += colocate_counts[c] // c
        print("{0:6d} are each occupied by {1:2d} waypoints.".format(colocate_counts[c] // c, c))
    print("Unique locations: " + str(unique_locations))

if args.errorcheck:
    print("!!! DATA CHECK SUCCESSFUL !!!")

print("Total run time: " + et.et())
