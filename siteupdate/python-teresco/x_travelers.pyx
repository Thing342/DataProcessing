import datetime
import re


class TravelerList:
    """This class encapsulates the contents of one .list file
    that represents the travels of one individual user.

    A list file consists of lines of 4 values:
    region route_name start_waypoint end_waypoint

    which indicates that the user has traveled the highway names
    route_name in the given region between the waypoints named
    start_waypoint end_waypoint
    """

    def __init__(self, travelername, route_hash, path="../../../UserData/list_files"):
        self.list_entries = []
        self.clinched_segments = set()
        self.traveler_name = travelername[:-5]
        with open(path + "/" + travelername, "rt", encoding='UTF-8') as file:
            lines = file.readlines()
        file.close()

        self.log_entries = []

        for line in lines:
            line = line.strip().rstrip('\x00')
            # ignore empty or "comment" lines
            if len(line) == 0 or line.startswith("#"):
                continue
            fields = re.split(' +', line)
            if len(fields) != 4:
                # OK if 5th field exists and starts with #
                if len(fields) < 5 or not fields[4].startswith("#"):
                    self.log_entries.append("Incorrect format line: " + line)
                    continue

            # find the root that matches in some system and when we do, match labels
            route_entry = fields[1].lower()
            lookup = fields[0].lower() + ' ' + route_entry
            if lookup not in route_hash:
                self.log_entries.append("Unknown region/highway combo in line: " + line)
            else:
                r = route_hash[lookup]
                for a in r.alt_route_names:
                    if route_entry == a.lower():
                        self.log_entries.append("Note: deprecated route name " + fields[
                            1] + " -> canonical name " + r.list_entry_name() + " in line " + line)
                        break

                if r.system.devel():
                    self.log_entries.append("Ignoring line matching highway in system in development: " + line)
                    continue
                # r is a route match, r.root is our root, and we need to find
                # canonical waypoint labels, ignoring case and leading
                # "+" or "*" when matching
                canonical_waypoints = []
                canonical_waypoint_indices = []
                checking_index = 0;
                for w in r.point_list:
                    lower_label = w.label.lower().strip("+*")
                    list_label_1 = fields[2].lower().strip("+*")
                    list_label_2 = fields[3].lower().strip("+*")
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
                                # if we have not yet used this alt label, remove it from the unused list
                                if lower_label.upper() in r.unused_alt_labels:
                                    r.unused_alt_labels.remove(lower_label.upper())

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
                    # start = r.point_list.index(canonical_waypoints[0])
                    # end = r.point_list.index(canonical_waypoints[1])
                    start = canonical_waypoint_indices[0]
                    end = canonical_waypoint_indices[1]
                    for wp_pos in range(start, end):
                        hs = r.segment_list[wp_pos]  # r.get_segment(r.point_list[wp_pos], r.point_list[wp_pos+1])
                        hs.add_clinched_by(self)
                        if hs not in self.clinched_segments:
                            self.clinched_segments.add(hs)

        self.log_entries.append("Processed " + str(len(self.list_entries)) + \
                                " good lines marking " + str(len(self.clinched_segments)) + \
                                " segments traveled.")
        # additional setup for later stats processing
        # a place to track this user's total mileage per region,
        # but only active+preview and active only (since devel
        # systems are not clinchable)
        self.active_preview_mileage_by_region = dict()
        self.active_only_mileage_by_region = dict()
        # a place for this user's total mileage per system, again by region
        # this will be a dictionary of dictionaries, keys of the top level
        # are system names (e.g., 'usai') and values are dictionaries whose
        # keys are region names and values are total mileage in that
        # system in that region
        self.system_region_mileages = dict()

    def write_log(self, path="."):
        logfile = open(path + "/" + self.traveler_name + ".log", "wt", encoding='UTF-8')
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

    def __init__(self, line, root, canonical_start, canonical_end):
        self.raw_line = line
        self.root = root
        self.canonical_start = canonical_start
        self.canonical_end = canonical_end
