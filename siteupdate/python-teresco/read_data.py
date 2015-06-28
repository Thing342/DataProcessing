#!/usr/bin/env python3
# Travel Mapping Project, Jim Teresco, 2015
"""Python code to read .csv and .wpt files and prepare for
adding to the Travel Mapping Project database.

(c) 2015, Jim Teresco

This module defines classes to represent the contents of a
.csv file that lists the highways within a system, and a
.wpt file that lists the waypoints for a given highway.
"""

import datetime
import math
import os
import re
import time

class ElapsedTime:
    """To get a nicely-formatted elapsed time string for printing"""

    def __init__(self):
        self.start_time = time.time()

    def et(self):
        return "[{0:.1f}] ".format(time.time()-self.start_time)

class WaypointQuadtree:
    """This class defines a recursive quadtree structure to store
    Waypoint objects for efficient geometric searching.
    """
    def __init__(self,min_lat,min_lng,max_lat,max_lng):
        """initialize an empty quadtree node on a given space"""
        self.min_lat = min_lat
        self.min_lng = min_lng
        self.max_lat = max_lat
        self.max_lng = max_lng
        self.mid_lat = (self.min_lat + self.max_lat) / 2
        self.mid_lng = (self.min_lng + self.max_lng) / 2
        self.nw_child = None
        self.ne_child = None
        self.sw_child = None
        self.se_child = None
        self.points = []

    def refine(self):
        """refine a quadtree into 4 sub-quadrants"""
        #print(str(self) + " being refined")
        self.nw_child = WaypointQuadtree(self.min_lat,self.mid_lng,self.mid_lat,self.max_lng)
        self.ne_child = WaypointQuadtree(self.mid_lat,self.mid_lng,self.max_lat,self.max_lng)
        self.sw_child = WaypointQuadtree(self.min_lat,self.min_lng,self.mid_lat,self.mid_lng)
        self.se_child = WaypointQuadtree(self.mid_lat,self.min_lng,self.max_lat,self.mid_lng)
        points = self.points
        self.points = None
        for p in points:
            self.insert(p)
        

    def insert(self,w):
        """insert Waypoint w into this quadtree node"""
        #print(str(self) + " insert " + str(w))
        if self.points is not None:
            self.points.append(w)
            if len(self.points) > 50:  # 50 points max per quadtree node
                self.refine()
        else:
            if w.lat < self.mid_lat:
                if w.lng < self.mid_lng:
                    self.sw_child.insert(w)
                else:
                    self.nw_child.insert(w)
            else:
                if w.lng < self.mid_lng:
                    self.se_child.insert(w)
                else:
                    self.ne_child.insert(w)

    def waypoint_at_same_point(self,w):
        """find an existing waypoint at the same coordinates as w"""
        if self.points is not None:
            for p in self.points:
                if p.same_coords(w):
                    return p
            return None
        else:
            if w.lat < self.mid_lat:
                if w.lng < self.mid_lng:
                    return self.sw_child.waypoint_at_same_point(w)
                else:
                    return self.nw_child.waypoint_at_same_point(w)
            else:
                if w.lng < self.mid_lng:
                    return self.se_child.waypoint_at_same_point(w)
                else:
                    return self.ne_child.waypoint_at_same_point(w)
            

    def __str__(self):
        s = "WaypointQuadtree at (" + str(self.min_lat) + "," + \
            str(self.min_lng) + ") to (" + str(self.max_lat) + "," + \
            str(self.max_lng) + ")"
        if self.points is None:
            return s + " REFINED"
        else:
            return s + " contains " + str(len(self.points)) + " waypoints"

    def size(self):
        """return the number of Waypoints in the tree"""
        if self.points is None:
            return self.nw_child.size() + self.ne_child.size() + self.sw_child.size() + self.se_child.size()
        else:
            return len(self.points)

    def point_list(self):
        """return a list of all points in the quadtree"""
        if self.points is None:
            all_points = []
            all_points.extend(self.ne_child.point_list())
            all_points.extend(self.nw_child.point_list())
            all_points.extend(self.se_child.point_list())
            all_points.extend(self.sw_child.point_list())
            return all_points
        else:
            return self.points

class Waypoint:
    """This class encapsulates the information about a single waypoint
    from a .wpt file.

    The line consists of one or more labels, at most one of which can
    be a "regular" label.  Others are "hidden" labels and must begin with
    a '+'.  Then an OSM URL which encodes the latitude and longitude.

    root is the unique identifier for the route in which this waypoint
    is defined
    """
    def __init__(self,line,route):
        """initialize object from a .wpt file line"""
        self.route = route
        parts = line.split()
        self.label = parts[0]
        self.is_hidden = self.label.startswith('+')
        if len(parts) > 2:
            # all except first and last
            self.alt_labels = parts[1:-1]
        else:
            self.alt_labels = []
        # last has the URL, which needs more work to get lat/lng
        url_parts = parts[-1].split('=')
        lat_string = url_parts[1].split("&")[0] # chop off "&lon"
        lng_string = url_parts[2].split("&")[0] # chop off possible "&zoom"
        self.lat = float(lat_string)
        self.lng = float(lng_string)
        # also keep track of a list of colocated waypoints, if any
        self.colocated = None

    def __str__(self):
        ans = self.route.root + " " + self.label
        if len(self.alt_labels) > 0:
            ans = ans + " [alt: " + str(self.alt_labels) + "]"
        ans = ans + " (" + str(self.lat) + "," + str(self.lng) + ")"
        return ans

    def sql_insert_command(self,tablename,id):
        """return sql command to insert into a table"""
        return "INSERT INTO " + tablename + " VALUES ('" + str(id) + "','" + self.label + "','" + str(self.lat) + "','" + str(self.lng) + "','" + self.route.root + "');"

    def csv_line(self,id):
        """return csv line to insert into a table"""
        return "'" + str(id) + "','" + self.label + "','" + str(self.lat) + "','" + str(self.lng) + "','" + self.route.root + "'"

    def same_coords(self,other):
        """return if this waypoint is colocated with the other,
        using exact lat,lng match"""
        return self.lat == other.lat and self.lng == other.lng

    def num_colocated(self):
        """return the number of points colocated with this one (including itself)"""
        if self.colocated is None:
            return 1
        else:
            return len(self.colocated)

    def distance_to(self,other):
        """return the distance in miles between this waypoint and another
        including the factor defined by the CHM project to adjust for
        unplotted curves in routes"""
        # convert to radians
        rlat1 = math.radians(self.lat)
        rlng1 = math.radians(self.lng)
        rlat2 = math.radians(other.lat)
        rlng2 = math.radians(other.lng)
        
        ans = math.acos(math.cos(rlat1)*math.cos(rlng1)*math.cos(rlat2)*math.cos(rlng2) +\
                        math.cos(rlat1)*math.sin(rlng1)*math.cos(rlat2)*math.sin(rlng2) +\
                        math.sin(rlat1)*math.sin(rlat2)) * 3963.1 # EARTH_RADIUS;
        return ans * 1.02112

    def angle(self,pred,succ):
        """return the angle in degrees formed by the waypoints between the
        line from pred to self and self to succ"""
        # convert to radians
        rlatself = math.radians(self.lat)
        rlngself = math.radians(self.lng)
        rlatpred = math.radians(pred.lat)
        rlngpred = math.radians(pred.lng)
        rlatsucc = math.radians(succ.lat)
        rlngsucc = math.radians(succ.lng)

        x0 = math.cos(rlngpred)*math.cos(rlatpred)
        x1 = math.cos(rlngself)*math.cos(rlatself)
        x2 = math.cos(rlngsucc)*math.cos(rlatsucc)

        y0 = math.sin(rlngpred)*math.cos(rlatpred)
        y1 = math.sin(rlngself)*math.cos(rlatself)
        y2 = math.sin(rlngsucc)*math.cos(rlatsucc)

        z0 = math.sin(rlatpred)
        z1 = math.sin(rlatself)
        z2 = math.sin(rlatsucc)

        return math.degrees(math.acos(((x2 - x1)*(x1 - x0) + (y2 - y1)*(y1 - y0) + (z2 - z1)*(z1 - z0)) / math.sqrt(((x2 - x1)*(x2 - x1) + (y2 - y1)*(y2 - y1) + (z2 - z1)*(z2 - z1)) * ((x1 - x0)*(x1 - x0) + (y1 - y0)*(y1 - y0) + (z1 - z0)*(z1 - z0)))))                              
        

class HighwaySegment:
    """This class represents one highway segment: the connection between two
    Waypoints connected by one or more routes"""

    def __init__(self,w1,w2,route):
        self.waypoint1 = w1
        self.waypoint2 = w2
        self.route = route
        self.concurrent = None
        self.clinched_by = []

    def __str__(self):
        return self.waypoint1.label + " to " + self.waypoint2.label + \
            " via " + self.route.root

    def add_clinched_by(self,traveler):
        if traveler not in self.clinched_by:
            self.clinched_by.append(traveler)
            return True
        else:
            return False

    def sql_insert_command(self,tablename,id):
        """return sql command to insert into a table"""
        return "INSERT INTO " + tablename + " VALUES ('" + str(id) + "','" + str(self.waypoint1.point_num) + "','" + str(self.waypoint2.point_num) + "','" + self.route.root + "');"

    def csv_line(self,id):
        """return csv line to insert into a table"""
        return "'" + str(id) + "','" + str(self.waypoint1.point_num) + "','" + str(self.waypoint2.point_num) + "','" + self.route.root + "'"

    def length(self):
        """return segment length in miles"""
        return self.waypoint1.distance_to(self.waypoint2)

class Route:
    """This class encapsulates the contents of one .csv file line
    that represents a highway within a system and the corresponding
    information from the route's .wpt.

    The format of the .csv file for a highway system is a set of
    semicolon-separated lines, each of which has 8 fields:

    System;Region;Route;Banner;Abbrev;City;Route;AltRouteNames

    The first line names these fields, subsequent lines exist,
    one per highway, with values for each field.

    System: the name of the highway system this route belongs to,
    normally the same as the name of the .csv file.

    Region: the project region or subdivision in which the
    route belongs.

    Route: the route name as would be specified in user lists

    Banner: the (optional) banner on the route such as 'Alt',
    'Bus', or 'Trk'.

    Abbrev: (optional) for bannered routes or routes in multiple
    sections, the 3-letter abbrevation for the city or other place
    that is used to identify the segment.

    City: (optional) the full name to be displayed for the Abbrev
    above.

    Root: the name of the .wpt file that lists the waypoints of the
    route, without the .wpt extension.

    AltRouteNames: (optional) comma-separated list former or other
    alternate route names that might appear in user list files.
    """
    def __init__(self,line,system):
        """initialize object from a .csv file line, but do not
        yet read in waypoint file"""
        self.line = line
        fields = line.split(";")
        if len(fields) != 8:
            print("Could not parse csv line: " + line)
        self.system = system
        if system.systemname != fields[0]:
            print("System mismatch parsing line [" + "], expected " + system.systemname)
        self.region = fields[1]
        self.route = fields[2]
        self.banner = fields[3]
        self.abbrev = fields[4]
        self.city = fields[5].replace("'","''")
        self.root = fields[6]
        self.alt_route_names = fields[7].split(",")
        self.point_list = []
        self.labels_in_use = set()
        self.segment_list = []
        self.mileage = 0.0

    def __str__(self):
        """printable version of the object"""
        return self.root + " (" + str(len(self.point_list)) + " total points)"

    def read_wpt(self,all_waypoints,path="../../../HighwayData/chm_final"):
        """read data into the Route's waypoint list from a .wpt file"""
        #print("read_wpt on " + str(self))
        self.point_list = []
        with open(path+"/"+self.system.systemname+"/"+self.root+".wpt", "rt",encoding='utf-8') as file:
            lines = file.readlines()
        w = None
        for line in lines:
            if len(line.rstrip('\n')) > 0:
                previous_point = w
                w = Waypoint(line.rstrip('\n'),self)
                self.point_list.append(w)
                # look for colocated points
                other_w = all_waypoints.waypoint_at_same_point(w)
                if other_w is not None:
                    # see if this is the first point colocated with other_w
                    if other_w.colocated is None:
                        other_w.colocated = [ other_w ]
                    other_w.colocated.append(w)
                    w.colocated = other_w.colocated
                    #print("New colocation found: " + str(w) + " with " + str(other_w))
                all_waypoints.insert(w)
                # add HighwaySegment, if not first point
                if previous_point is not None:
                    self.segment_list.append(HighwaySegment(previous_point, w, self))

    def print_route(self):
        for point in self.point_list:
            print(str(point))

    def find_segment_by_waypoints(self,w1,w2):
        for s in self.segment_list:
            if s.waypoint1 is w1 and s.waypoint2 is w2 or s.waypoint1 is w2 and s.waypoint2 is w1:
                return s
        return None

    def sql_insert_command(self,tablename):
        """return sql command to insert into a table"""
        # note: alt_route_names does not need to be in the db since
        # list preprocessing uses alt or canonical and no longer cares
        return "INSERT INTO " + tablename + " VALUES ('" + self.system.systemname + "','" + self.region + "','" + self.route + "','" + self.banner + "','" + self.abbrev + "','" + self.city + "','" + self.root + "');";

    def csv_line(self):
        """return csv line to insert into a table"""
        # note: alt_route_names does not need to be in the db since
        # list preprocessing uses alt or canonical and no longer cares
        return "'" + self.system.systemname + "','" + self.region + "','" + self.route + "','" + self.banner + "','" + self.abbrev + "','" + self.city + "','" + self.root + "'";

    def readable_name(self):
        """return a string for a human-readable route name"""
        return self.region+ " " + self.route + self.banner + self.abbrev

class HighwaySystem:
    """This class encapsulates the contents of one .csv file
    that represents the collection of highways within a system.

    See Route for information about the fields of a .csv file

    Each HighwaySystem is also designated as active or inactive via
    the parameter active, defaulting to true
    """
    def __init__(self,systemname,country,fullname,color,tier,active,path="../../../HighwayData/chm_final/_systems"):
        self.route_list = []
        self.systemname = systemname
        self.country = country
        self.fullname = fullname
        self.color = color
        self.tier = tier
        self.active = active
        self.mileage = 0.0
        with open(path+"/"+systemname+".csv","rt",encoding='utf-8') as file:
            lines = file.readlines()
        # ignore the first line of field names
        lines.pop(0)
        for line in lines:
            self.route_list.append(Route(line.rstrip('\n'),self))
        file.close()

class TravelerList:
    """This class encapsulates the contents of one .list file
    that represents the travels of one individual user.

    A list file consists of lines of 4 values:
    region route_name start_waypoint end_waypoint

    which indicates that the user has traveled the highway names
    route_name in the given region between the waypoints named
    start_waypoint end_waypoint
    """

    def __init__(self,travelername,systems,path="../../../UserData/list_files"):
        self.list_entries = []
        self.clinched_segments = set()
        self.traveler_name = travelername[:-5]
        with open(path+"/"+travelername,"rt", encoding='UTF-8') as file:
            lines = file.readlines()
        file.close()

        print("Processing " + travelername)

        self.log_entries = []

        for line in lines:
            line = line.strip()
            # ignore empty or "comment" lines
            if len(line) == 0 or line.startswith("#"):
                continue
            fields = re.split(' +',line)
            if len(fields) != 4:
                self.log_entries.append("Incorrect format line: " + line)
                continue

            # find the root that matches in some system and when we do, match labels
            lineDone = False
            for h in systems:
                for r in h.route_list:
                    if r.region.lower() != fields[0].lower():
                        continue
                    route_entry = fields[1].lower()
                    route_match = False
                    for a in r.alt_route_names:
                        if route_entry == a.lower():
                            self.log_entries.append("Note: replacing deprecated route name " + fields[1] + " with canonical name " + r.route + r.banner + r.abbrev + " in line " + line)
                            route_match = True
                            break
                    if route_match or (r.route + r.banner + r.abbrev).lower() == route_entry:
                        lineDone = True  # we'll either have success or failure here
                        if not h.active:
                            self.log_entries.append("Ignoring line matching highway in inactive system: " + line)
                            break
                        #print("Route match with " + str(r))
                        # r is a route match, r.root is our root, and we need to find
                        # canonical waypoint labels, ignoring case and leading "+" or "*" when matching
                        canonical_waypoints = []
                        canonical_waypoint_indices = []
                        checking_index = 0;
                        for w in r.point_list:
                            lower_label = w.label.lower().strip("+*")
                            list_label_1 = fields[2].lower().strip("*")
                            list_label_2 = fields[3].lower().strip("*")
                            if list_label_1 == lower_label or list_label_2 == lower_label:
                                canonical_waypoints.append(w)
                                canonical_waypoint_indices.append(checking_index)
                                r.labels_in_use.add(lower_label.upper())
                            else:
                                for alt in w.alt_labels:
                                    lower_label = alt.lower().strip("+")
                                    if list_label_1 == lower_label or list_label_2 == lower_label:
                                        canonical_waypoints.append(w)
                                        canonical_waypoint_indices.append(checking_index)
                                        r.labels_in_use.add(lower_label.upper())
                            checking_index += 1
                        if len(canonical_waypoints) != 2:
                            self.log_entries.append("Waypoint label(s) not found in line: " + line)
                        else:
                            self.list_entries.append(ClinchedSegmentEntry(line, r.root, \
                                                                          canonical_waypoints[0].label, \
                                                                          canonical_waypoints[1].label))
                            # find the segments we just matched and store this traveler with the
                            # segments and the segments with the traveler (might not need both
                            # ultimately)
                            #start = r.point_list.index(canonical_waypoints[0])
                            #end = r.point_list.index(canonical_waypoints[1])
                            start = canonical_waypoint_indices[0]
                            end = canonical_waypoint_indices[1]
                            for wp_pos in range(start,end):
                                hs = r.segment_list[wp_pos] #r.get_segment(r.point_list[wp_pos], r.point_list[wp_pos+1])
                                hs.add_clinched_by(self)
                                if hs not in self.clinched_segments:
                                    self.clinched_segments.add(hs)
                    
                if lineDone:
                    break
            if not lineDone:
                self.log_entries.append("Unknown region/highway combo in line: " + line)
        self.log_entries.append("Processed " + str(len(self.list_entries)) + \
                                    " good lines marking " +str(len(self.clinched_segments)) + \
                                    " segments traveled.")
       
    def write_log(self,path="."):
        logfile = open(path+"/"+self.traveler_name+".log","wt",encoding='UTF-8')
        logfile.write("Log file created at: " + str(datetime.datetime.now()) + "\n")
        for line in self.log_entries:
            logfile.write(line + "\n")
        logfile.close()

class ClinchedSegmentEntry:
    """This class encapsulates one line of a traveler's list file

    raw_line is the actual line from the list file for error reporting
    
    root is the root name of the route clinched

    canonical_start and canonical_end are waypoint labels, which must be
    in the same order as they appear in the route decription file, and
    must be primary labels
    """

    def __init__(self,line,root,canonical_start,canonical_end):
        self.raw_line = line
        self.root = root
        self.canonical_start = canonical_start
        self.canonical_end = canonical_end


#route_example = Route("usai;NY;I-684;;Pur;Purchase, NY;ny.i684pur;I-684_S")
#print(route_example)
#hs_example = HighwaySystem("usade")
#print(*hs_example.route_list)

# Execution code starts here
#
# start a timer for including elapsed time reports in messages
et = ElapsedTime()
#
# Also list of travelers in the system
#traveler_ids = [ 'terescoj', 'Bickendan', 'drfrankenstein', 'imgoph', 'master_son',
#                 'mojavenc', 'oscar', 'rickmastfan67', 'sammi', 'si404',
#                 'sipes23', 'froggie', 'mapcat', 'duke87', 'vdeane', 
#                 'johninkingwood', 'yakra', 'michih' ]
#traveler_ids = [ 'terescoj', 'si404' ]
traveler_ids = os.listdir('../../../UserData/list_files')

# Create a list of HighwaySystem objects, one per system in systems.csv file
highway_systems = []
print(et.et() + "Reading systems list.  ",end="",flush=True)
with open("../../../HighwayData/systems.csv", "rt") as file:
    lines = file.readlines()

lines.pop(0)  # ignore header line for now
for line in lines:
    fields = line.rstrip('\n').split(";")
    if len(fields) != 6:
        print("Could not parse csv line: " + line)
    print(fields[0] + ".",end="",flush=True)
    highway_systems.append(HighwaySystem(fields[0], fields[1], fields[2].replace("'","''"),\
                                         fields[3], fields[4], fields[5] != 'yes'))
print("")

#for h in active_systems:
#    highway_systems.append(HighwaySystem(h))
#for h in devel_systems:
#    highway_systems.append(HighwaySystem(h,active=False))

# For finding colocated Waypoints and concurrent segments, we have a list
# of all Waypoints in existence
# This is a definite candidate for a more efficient data structure -- 
# a quadtree might make a lot of sense
all_waypoints = WaypointQuadtree(-180,-90,180,90)

print(et.et() + "Reading waypoints for all routes.")
# Next, read all of the .wpt files for each HighwaySystem
for h in highway_systems:
    print(h.systemname,end="",flush=True)
    for r in h.route_list:
        r.read_wpt(all_waypoints)
        print(".", end="",flush=True)
        #print(str(r))
        #r.print_route()
    print("!")

# data check: visit each system and route and check for various problems
# write to log file for now, maybe should be in DB later
print(et.et() + "Performing data checks.")
datacheckfile = open('datacheck.log','w',encoding='utf-8')
for h in highway_systems:
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
        for w in r.point_list:
            # duplicate labels
            label_list = w.alt_labels.copy()
            label_list.append(w.label)
            for label in label_list:
                lower_label = label.lower().strip("+*")
                if lower_label in all_route_labels:
                    datacheckfile.write(r.readable_name() + \
                                        " duplicate label " + lower_label + '\n')
                else:
                    all_route_labels.add(lower_label)
            # duplicate coordinates
            latlng = w.lat, w.lng 
            if latlng in coords_used:
                datacheckfile.write(r.readable_name() + " duplicate coordinates (" + \
                                    str(latlng[0]) + "," + str(latlng[1]) + \
                                    ")\n")
            else:
                coords_used.add(latlng)

            # visible distance update, and last segment length check
            if prev_w is not None:
                last_distance = w.distance_to(prev_w)
                visible_distance += last_distance
                if last_distance > 20.0:
                    datacheckfile.write(r.readable_name() + " " + prev_w.label + \
                                        ' ' + w.label + " long segment " + \
                                        "({0:.2f} mi)".format(last_distance) + "\n")

            if not w.is_hidden:
                # complete visible distance check
                if visible_distance > 10.0:
                    datacheckfile.write(r.readable_name() + " " + last_visible.label + \
                                        ' ' + w.label + " distance between visible " + \
                                        "waypoints too long " + \
                                        "({0:.2f} mi)".format(visible_distance) + "\n")
                last_visible = w
                visible_distance = 0.0

                # looking for the route within the label
                #match_start = w.label.find(r.route)
                #if match_start >= 0:
                    # we have a potential match, just need to make sure if the route
                    # name ends with a number that the matched substring isn't followed
                    # by more numbers (e.g., NY50 is an OK label in NY5)
                #    if len(r.route) + match_start == len(w.label) or \
                #            not w.label[len(r.route) + match_start].isdigit():
                #        datacheckfile.write(r.readable_name() + " " + w.label + \
                #                                " label references own route\n")
                # partially complete "references own route" -- too many FPs
                if r.route+r.banner == w.label:
                    datacheckfile.write(r.readable_name() + " " + w.label + \
                                        " label references own route\n")
                # look for old "0" or "999" labels
                for num in ['0','999']:
                    if w.label.startswith(num) or '('+num+')' in w.label:
                        datacheckfile.write(r.readable_name() + " " + w.label + \
                                            " might not refer to an exit " + num + '\n')

                # look for too many underscores in label
                if w.label.count('_') > 1:
                    datacheckfile.write(r.readable_name() + " " + w.label + \
                                        ' has too many underscored suffixes\n')

                # look for too many characters after underscore in label
                if '_' in w.label:
                    if w.label.index('_') < len(w.label) - 5:
                        datacheckfile.write(r.readable_name() + " " + w.label + \
                                            ' has long underscore suffix\n')

                # look for too many slashes in label
                if w.label.count('/') > 1:
                    datacheckfile.write(r.readable_name() + " " + w.label + \
                                        ' has too many slashes\n')

                # look for parenthesis balance in label
                if w.label.count('(') != w.label.count(')'):
                    datacheckfile.write(r.readable_name() + " " + w.label + \
                                        ' had parenthesis imbalance\n')

                # look for labels with invalid characters
                if not re.fullmatch('[a-zA-Z0-9()/\+\*_\-\.]+', w.label):
                    datacheckfile.write(r.readable_name() + " " + w.label + \
                                        ' includes invalid characters\n')

                # look for labels with a slash after an underscore
                if '_' in w.label and '/' in w.label and \
                        w.label.index('/') > w.label.index('_'):
                    datacheckfile.write(r.readable_name() + " " + w.label + \
                                        ' label has nonterminal underscore suffix\n')

                # look for I-xx with Bus instead of BL or BS
                if re.fullmatch('I\-[0-9]*Bus', w.label):
                    datacheckfile.write(r.readable_name() + " " + w.label + \
                                        ' label uses Bus with I- (Interstate)\n')

                # look for USxxxA but not USxxxAlt, B/Bus (others?)
                if re.fullmatch('US[0-9]+A.*', w.label) and not re.fullmatch('US[0-9]+Alt.*', w.label) or \
                   re.fullmatch('US[0-9]+B.*', w.label) and \
                   not (re.fullmatch('US[0-9]+Bus.*', w.label) or re.fullmatch('US[0-9]+Byp.*', w.label)):
                    datacheckfile.write(r.readable_name() + " " + w.label + \
                                        ' uses an incorrect banner with US\n')

            prev_w = w

        # angle check is easier with a traditional for loop and array indices
        for i in range(1, len(r.point_list)-1):
            #print("computing angle for " + str(r.point_list[i-1]) + ' ' + str(r.point_list[i]) + ' ' + str(r.point_list[i+1]))
            if r.point_list[i-1].same_coords(r.point_list[i]) or \
               r.point_list[i+1].same_coords(r.point_list[i]):
                datacheckfile.write(r.readable_name() + ' ' + r.point_list[i-1].label + \
                                    ' ' + r.point_list[i].label + ' ' + \
                                    r.point_list[i+1].label + ' angle not computable\n')
            else:
                angle = r.point_list[i].angle(r.point_list[i-1],r.point_list[i+1])
                if angle > 135:
                    datacheckfile.write(r.readable_name() + ' ' + r.point_list[i-1].label + \
                                        ' ' + r.point_list[i].label + ' ' + \
                                        r.point_list[i+1].label + ' sharp angle ' + \
                                        "{0:.2f} deg.".format(angle) + "\n")

datacheckfile.close()

# Create a list of TravelerList objects, one per person
traveler_lists = []

print(et.et() + "Processing traveler list files.")
for t in traveler_ids:
    if t.endswith('.list'):
        traveler_lists.append(TravelerList(t,highway_systems))

# write log file for points in use -- might be more useful in the DB later,
# or maybe in another format
print(et.et() + "Writing points in use log.")
inusefile = open('pointsinuse.log','w',encoding='UTF-8')
for h in highway_systems:
    for r in h.route_list:
        if len(r.labels_in_use) > 0:
            inusefile.write("Labels in use for " + str(r) + ": " + str(r.labels_in_use) + "\n")
inusefile.close()

# concurrency detection -- will augment our structure with list of concurrent
# segments with each segment (that has a concurrency)
print(et.et() + "Concurrent segment detection.",end="",flush=True)
concurrencyfile = open('concurrencies.log','w',encoding='UTF-8')
for h in highway_systems:
    print(".",end="",flush=True)
    for r in h.route_list:
        for s in r.segment_list:
            if s.waypoint1.colocated is not None and s.waypoint2.colocated is not None:
                for w1 in s.waypoint1.colocated:
                    if w1.route is not r:
                        for w2 in s.waypoint2.colocated:
                            if w1.route is w2.route:
                                other = w1.route.find_segment_by_waypoints(w1,w2)
                                if other is not None:
                                    if s.concurrent is None:
                                        s.concurrent = []
                                        other.concurrent = s.concurrent
                                        s.concurrent.append(s)
                                        s.concurrent.append(other)
                                        concurrencyfile.write("New concurrency [" + str(s) + "][" + str(other) + "] (" + str(len(s.concurrent)) + ")\n")
                                    else:
                                        if other not in s.concurrent:
                                            s.concurrent.append(other)
                                            #concurrencyfile.write("Added concurrency [" + str(s) + "]-[" + str(other) + "] ("+ str(len(s.concurrent)) + ")\n")
                                            concurrencyfile.write("Extended concurrency ")
                                            for x in s.concurrent:
                                                concurrencyfile.write("[" + str(x) + "]")
                                            concurrencyfile.write(" (" + str(len(s.concurrent)) + ")\n")
print("!")

# now augment any traveler clinched segments for concurrencies

print(et.et() + "Augmenting travelers for detected concurrent segments.",end="",flush=True)
for t in traveler_lists:
    print(".",end="",flush=True)
    for s in t.clinched_segments:
        if s.concurrent is not None:
            for hs in s.concurrent:
                if hs.route.system.active and hs.add_clinched_by(t):
                    concurrencyfile.write("Concurrency augment for traveler " + t.traveler_name + ": [" + str(hs) + "] based on [" + str(s) + "]\n")
print("!")
concurrencyfile.close()

# compute lots of stats, first total mileage by route, system
print(et.et() + "Computing stats.")
for h in highway_systems:
    for r in h.route_list:
        for s in r.segment_list:
            segment_length = s.length()
            r.mileage += segment_length
        #print(r.root + " {0:.2f} mi".format(r.mileage))

# write log files for traveler lists
print(et.et() + "Writing traveler list logs.")
for t in traveler_lists:
    t.write_log()

print(et.et() + "Writing database file.")
# Once all data is read in and processed, create a .sql file that will 
# create all of the DB tables to be used by other parts of the project
sqlfile = open('siteupdate.sql','w',encoding='UTF-8')
sqlfile.write('USE TravelMapping\n')

# we have to drop tables in the right order to avoid foreign key errors
sqlfile.write('DROP TABLE IF EXISTS clinched;\n')
sqlfile.write('DROP TABLE IF EXISTS segments;\n')
sqlfile.write('DROP TABLE IF EXISTS wpAltNames;\n')
sqlfile.write('DROP TABLE IF EXISTS waypoints;\n')
sqlfile.write('DROP TABLE IF EXISTS routes;\n')
sqlfile.write('DROP TABLE IF EXISTS systems;\n')

# first, a table of the systems, consisting of the system name in the
# field 'name', the system's country code, its full name, the default
# color for its mapping, and a boolean indicating if the system is
# active for mapping in the project in the field 'active'
sqlfile.write('CREATE TABLE systems (systemName VARCHAR(10), countryCode CHAR(3), fullName VARCHAR(50), color VARCHAR(10), tier INTEGER, active BOOLEAN, PRIMARY KEY(systemName));\n')
sqlfile.write('INSERT INTO systems VALUES\n')
first = True
for h in highway_systems:
    active = 0;
    if h.active:
        active = 1;
    if not first:
        sqlfile.write(",")
    first = False
    sqlfile.write("('" + h.systemname + "','" +  h.country + "','" +  h.fullname + "','" + \
                      h.color + "','" + str(h.tier) + "','" + str(active) + "')\n")
sqlfile.write(";\n")

# next, a table of highways, with the same fields as in the first line
sqlfile.write('CREATE TABLE routes (systemName VARCHAR(10), region VARCHAR(3), route VARCHAR(16), banner VARCHAR(3), abbrev VARCHAR(3), city VARCHAR(32), root VARCHAR(32), PRIMARY KEY(root), FOREIGN KEY (systemName) REFERENCES systems(systemName));\n')
sqlfile.write('INSERT INTO routes VALUES\n')
first = True
for h in highway_systems:
    for r in h.route_list:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write("(" + r.csv_line() + ")\n")
sqlfile.write(";\n")

# Now, a table with raw highway route data

sqlfile.write('CREATE TABLE waypoints (pointId INTEGER, pointName VARCHAR(20), latitude DOUBLE, longitude DOUBLE, root VARCHAR(32), PRIMARY KEY(pointId), FOREIGN KEY (root) REFERENCES routes(root));\n')
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
            point_num+=1
        sqlfile.write(";\n")

# Table of all HighwaySegments.  Good?  TBD.  Put in traveler clinches too, but need to keep in a
# list to make these one large query each
sqlfile.write('CREATE TABLE segments (segmentId INTEGER, waypoint1 INTEGER, waypoint2 INTEGER, root VARCHAR(32), PRIMARY KEY (segmentId), FOREIGN KEY (waypoint1) REFERENCES waypoints(pointId), FOREIGN KEY (waypoint2) REFERENCES waypoints(pointId), FOREIGN KEY (root) REFERENCES routes(root));\n')
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
sqlfile.write('CREATE TABLE clinched (segmentId INTEGER, traveler VARCHAR(48), FOREIGN KEY (segmentId) REFERENCES segments(segmentId));\n')
for start in range(0, len(clinched_list), 10000):
    sqlfile.write('INSERT INTO clinched VALUES\n')
    first = True
    for c in clinched_list[start:start+10000]:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write("(" + c + ")\n")
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
# compute colocation of waypoints stats
colocate_counts = [0]*50
largest_colocate_count = 1
for w in all_waypoints.point_list():
    c = w.num_colocated()
    #if c == 10:
    #    print(str(w))
    colocate_counts[c] += 1
    if c > largest_colocate_count:
        largest_colocate_count = c
print("Waypoint colocation counts:")
unique_locations = 0
for c in range(1,largest_colocate_count+1):
    unique_locations += colocate_counts[c]//c
    print(str(colocate_counts[c]//c) + " are each occupied by " + str(c) + " waypoints.")
print("Unique locations: " + str(unique_locations))