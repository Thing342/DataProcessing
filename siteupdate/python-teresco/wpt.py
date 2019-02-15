import math
import threading


class Waypoint:
    """This class encapsulates the information about a single waypoint
    from a .wpt file.

    The line consists of one or more labels, at most one of which can
    be a "regular" label.  Others are "hidden" labels and must begin with
    a '+'.  Then an OSM URL which encodes the latitude and longitude.

    root is the unique identifier for the route in which this waypoint
    is defined
    """

    def __init__(self, line, route, datacheckerrors):
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
        if len(url_parts) < 3:
            # print("\nWARNING: Malformed URL in " + route.root + ", line: " + line, end="", flush=True)
            datacheckerrors.append(DatacheckEntry(route, [self.label], 'MALFORMED_URL', parts[-1]))
            self.lat = 0
            self.lng = 0
            self.colocated = None
            self.near_miss_points = None
            return
        lat_string = url_parts[1].split("&")[0]  # chop off "&lon"
        lng_string = url_parts[2].split("&")[0]  # chop off possible "&zoom"

        # make sure lat_string is valid
        point_count = 0
        for c in range(len(lat_string)):
            # check for multiple decimal points
            if lat_string[c] == '.':
                point_count += 1
                if point_count > 1:
                    # print("\nWARNING: Malformed URL in " + route.root + ", line: " + line, end="", flush=True)
                    datacheckerrors.append(DatacheckEntry(route, [self.label], 'MALFORMED_URL', parts[-1]))
                    lat_string = "0"
                    lng_string = "0"
                    break
            # check for minus sign not at beginning
            if lat_string[c] == '-' and c > 0:
                # print("\nWARNING: Malformed URL in " + route.root + ", line: " + line, end="", flush=True)
                datacheckerrors.append(DatacheckEntry(route, [self.label], 'MALFORMED_URL', parts[-1]))
                lat_string = "0"
                lng_string = "0"
                break
            # check for invalid characters
            if lat_string[c] not in "-.0123456789":
                # print("\nWARNING: Malformed URL in " + route.root + ", line: " + line, end="", flush=True)
                datacheckerrors.append(DatacheckEntry(route, [self.label], 'MALFORMED_URL', parts[-1]))
                lat_string = "0"
                lng_string = "0"
                break

        # make sure lng_string is valid
        point_count = 0
        for c in range(len(lng_string)):
            # check for multiple decimal points
            if lng_string[c] == '.':
                point_count += 1
                if point_count > 1:
                    # print("\nWARNING: Malformed URL in " + route.root + ", line: " + line, end="", flush=True)
                    datacheckerrors.append(DatacheckEntry(route, [self.label], 'MALFORMED_URL', parts[-1]))
                    lat_string = "0"
                    lng_string = "0"
                    break
            # check for minus sign not at beginning
            if lng_string[c] == '-' and c > 0:
                # print("\nWARNING: Malformed URL in " + route.root + ", line: " + line, end="", flush=True)
                datacheckerrors.append(DatacheckEntry(route, [self.label], 'MALFORMED_URL', parts[-1]))
                lat_string = "0"
                lng_string = "0"
                break
            # check for invalid characters
            if lng_string[c] not in "-.0123456789":
                # print("\nWARNING: Malformed URL in " + route.root + ", line: " + line, end="", flush=True)
                datacheckerrors.append(DatacheckEntry(route, [self.label], 'MALFORMED_URL', parts[-1]))
                lat_string = "0"
                lng_string = "0"
                break

        self.lat = float(lat_string)
        self.lng = float(lng_string)
        # also keep track of a list of colocated waypoints, if any
        self.colocated = None
        # and keep a list of "near-miss points", if any
        self.near_miss_points = None

    def __str__(self):
        ans = self.route.root + " " + self.label
        if len(self.alt_labels) > 0:
            ans = ans + " [alt: " + str(self.alt_labels) + "]"
        ans = ans + " (" + str(self.lat) + "," + str(self.lng) + ")"
        return ans

    def csv_line(self, id):
        """return csv line to insert into a table"""
        return "'" + str(id) + "','" + self.label + "','" + str(self.lat) + "','" + str(
            self.lng) + "','" + self.route.root + "'"

    def same_coords(self, other):
        """return if this waypoint is colocated with the other,
        using exact lat,lng match"""
        return self.lat == other.lat and self.lng == other.lng

    def nearby(self, other, tolerance):
        """return if this waypoint's coordinates are within the given
        tolerance (in degrees) of the other"""
        return abs(self.lat - other.lat) < tolerance and \
               abs(self.lng - other.lng) < tolerance

    def num_colocated(self):
        """return the number of points colocated with this one (including itself)"""
        if self.colocated is None:
            return 1
        else:
            return len(self.colocated)

    def distance_to(self, other):
        """return the distance in miles between this waypoint and another
        including the factor defined by the CHM project to adjust for
        unplotted curves in routes"""
        # convert to radians
        rlat1 = math.radians(self.lat)
        rlng1 = math.radians(self.lng)
        rlat2 = math.radians(other.lat)
        rlng2 = math.radians(other.lng)

        ans = math.acos(math.cos(rlat1) * math.cos(rlng1) * math.cos(rlat2) * math.cos(rlng2) + \
                        math.cos(rlat1) * math.sin(rlng1) * math.cos(rlat2) * math.sin(rlng2) + \
                        math.sin(rlat1) * math.sin(rlat2)) * 3963.1  # EARTH_RADIUS;
        return ans * 1.02112

    def angle(self, pred, succ):
        """return the angle in degrees formed by the waypoints between the
        line from pred to self and self to succ"""
        # convert to radians
        rlatself = math.radians(self.lat)
        rlngself = math.radians(self.lng)
        rlatpred = math.radians(pred.lat)
        rlngpred = math.radians(pred.lng)
        rlatsucc = math.radians(succ.lat)
        rlngsucc = math.radians(succ.lng)

        x0 = math.cos(rlngpred) * math.cos(rlatpred)
        x1 = math.cos(rlngself) * math.cos(rlatself)
        x2 = math.cos(rlngsucc) * math.cos(rlatsucc)

        y0 = math.sin(rlngpred) * math.cos(rlatpred)
        y1 = math.sin(rlngself) * math.cos(rlatself)
        y2 = math.sin(rlngsucc) * math.cos(rlatsucc)

        z0 = math.sin(rlatpred)
        z1 = math.sin(rlatself)
        z2 = math.sin(rlatsucc)

        return math.degrees(math.acos(
            ((x2 - x1) * (x1 - x0) + (y2 - y1) * (y1 - y0) + (z2 - z1) * (z1 - z0)) / math.sqrt(
                ((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1) + (z2 - z1) * (z2 - z1)) * (
                            (x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0) + (z1 - z0) * (z1 - z0)))))

    def canonical_waypoint_name(self, log):
        """Best name we can come up with for this point bringing in
        information from itself and colocated points (if active/preview)
        """
        # start with the failsafe name, and see if we can improve before
        # returning
        name = self.simple_waypoint_name()

        # if no colocated points, there's nothing to do - we just use
        # the route@label form and deal with conflicts elsewhere
        if self.colocated is None:
            return name

        # get a colocated list that any devel system entries removed
        colocated = []
        for w in self.colocated:
            if w.route.system.active_or_preview():
                colocated.append(w)
        # just return the simple name if only one active/preview waypoint
        if (len(colocated) == 1):
            return name

        # straightforward concurrency example with matching waypoint
        # labels, use route/route/route@label, except also matches
        # any hidden label
        # TODO: compress when some but not all labels match, such as
        # E127@Kan&AH60@Kan_N&AH64@Kan&AH67@Kan&M38@Kan
        # or possibly just compress ignoring the _ suffixes here
        routes = ""
        pointname = ""
        matches = 0
        for w in colocated:
            if routes == "":
                routes = w.route.list_entry_name()
                pointname = w.label
                matches = 1
            elif pointname == w.label or w.label.startswith('+'):
                # this check seems odd, but avoids double route names
                # at border crossings
                if routes != w.route.list_entry_name():
                    routes += "/" + w.route.list_entry_name()
                matches += 1
        if matches == len(colocated):
            log.append("Straightforward concurrency: " + name + " -> " + routes + "@" + pointname)
            return routes + "@" + pointname

        # straightforward 2-route intersection with matching labels
        # NY30@US20&US20@NY30 would become NY30/US20
        # or
        # 2-route intersection with one or both labels having directional
        # suffixes but otherwise matching route
        # US24@CO21_N&CO21@US24_E would become US24_E/CO21_N

        if len(colocated) == 2:
            w0_list_entry = colocated[0].route.list_entry_name()
            w1_list_entry = colocated[1].route.list_entry_name()
            w0_label = colocated[0].label
            w1_label = colocated[1].label
            if (w0_list_entry == w1_label or \
                w1_label.startswith(w0_list_entry + '_')) and \
                    (w1_list_entry == w0_label or \
                     w0_label.startswith(w1_list_entry + '_')):
                log.append("Straightforward intersection: " + name + " -> " + w1_label + '/' + w0_label)
                return w1_label + '/' + w0_label

        # check for cases like
        # I-10@753B&US90@I-10(753B)
        # which becomes
        # I-10(753B)/US90
        # more generally,
        # I-30@135&US67@I-30(135)&US70@I-30(135)
        # becomes
        # I-30(135)/US67/US70
        # but also matches some other cases that perhaps should
        # be checked or handled separately, though seems OK
        # US20@NY30A&NY30A@US20&NY162@US20
        # becomes
        # US20/NY30A/NY162

        for match_index in range(0, len(colocated)):
            lookfor1 = colocated[match_index].route.list_entry_name()
            lookfor2 = colocated[match_index].route.list_entry_name() + \
                       '(' + colocated[match_index].label + ')'
            all_match = True
            for check_index in range(0, len(colocated)):
                if match_index == check_index:
                    continue
                if (colocated[check_index].label != lookfor1) and \
                        (colocated[check_index].label != lookfor2):
                    all_match = False
            if all_match:
                if (colocated[match_index].label[0:1].isnumeric()):
                    label = lookfor2
                else:
                    label = lookfor1
                for add_index in range(0, len(colocated)):
                    if match_index == add_index:
                        continue
                    label += '/' + colocated[add_index].route.list_entry_name()
                log.append("Exit/Intersection: " + name + " -> " + label)
                return label

        # TODO: NY5@NY16/384&NY16@NY5/384&NY384@NY5/16
        # should become NY5/NY16/NY384

        # 3+ intersection with matching or partially matching labels
        # NY5@NY16/384&NY16@NY5/384&NY384@NY5/16
        # becomes NY5/NY16/NY384

        # or a more complex case:
        # US1@US21/176&US21@US1/378&US176@US1/378&US321@US1/378&US378@US21/176
        # becomes US1/US21/US176/US321/US378
        # approach: check if each label starts with some route number
        # in the list of colocated routes, and if so, create a label
        # slashing together all of the route names, and save any _
        # suffixes to put in and reduce the chance of conflicting names
        # and a second check to find matches when labels do not include
        # the abbrev field (which they often do not)
        if len(colocated) > 2:
            all_match = True
            suffixes = [""] * len(colocated)
            for check_index in range(len(colocated)):
                this_match = False
                for other_index in range(len(colocated)):
                    if other_index == check_index:
                        continue
                    if colocated[check_index].label.startswith(colocated[other_index].route.list_entry_name()):
                        # should check here for false matches, like
                        # NY50/67 would match startswith NY5
                        this_match = True
                        if '_' in colocated[check_index].label:
                            suffix = colocated[check_index].label[colocated[check_index].label.find('_'):]
                            if colocated[other_index].route.list_entry_name() + suffix == colocated[check_index].label:
                                suffixes[other_index] = suffix
                    if colocated[check_index].label.startswith(colocated[other_index].route.name_no_abbrev()):
                        this_match = True
                        if '_' in colocated[check_index].label:
                            suffix = colocated[check_index].label[colocated[check_index].label.find('_'):]
                            if colocated[other_index].route.name_no_abbrev() + suffix == colocated[check_index].label:
                                suffixes[other_index] = suffix
                if not this_match:
                    all_match = False
                    break
            if all_match:
                label = colocated[0].route.list_entry_name() + suffixes[0]
                for index in range(1, len(colocated)):
                    label += "/" + colocated[index].route.list_entry_name() + suffixes[index]
                log.append("3+ intersection: " + name + " -> " + label)
                return label

        # Exit number simplification: I-90@47B(94)&I-94@47B
        # becomes I-90/I-94@47B, with many other cases also matched
        # Still TODO: I-39@171C(90)&I-90@171C&US14@I-39/90
        # try each as a possible route@exit type situation and look
        # for matches
        for try_as_exit in range(len(colocated)):
            # see if all colocated points are potential matches
            # when considering the one at try_as_exit as a primary
            # exit number
            if not colocated[try_as_exit].label[0].isdigit():
                continue
            all_match = True
            # get the route number only version for one of the checks below
            route_number_only = colocated[try_as_exit].route.name_no_abbrev()
            for pos in range(len(route_number_only)):
                if route_number_only[pos].isdigit():
                    route_number_only = route_number_only[pos:]
                    break
            for try_as_match in range(len(colocated)):
                if try_as_exit == try_as_match:
                    continue
                this_match = False
                # check for any of the patterns that make sense as a match:
                # exact match, match without abbrev field, match with exit
                # number in parens, match concurrency exit number format
                # nn(rr), match with _ suffix (like _N), match with a slash
                # match with exit number only
                if (colocated[try_as_match].label == colocated[try_as_exit].route.list_entry_name()
                        or colocated[try_as_match].label == colocated[try_as_exit].route.name_no_abbrev()
                        or colocated[try_as_match].label == colocated[try_as_exit].route.list_entry_name() + "(" +
                        colocated[try_as_exit].label + ")"
                        or colocated[try_as_match].label == colocated[try_as_exit].label + "(" + route_number_only + ")"
                        or colocated[try_as_match].label == colocated[try_as_exit].label + "(" + colocated[
                            try_as_exit].route.name_no_abbrev() + ")"
                        or colocated[try_as_match].label.startswith(colocated[try_as_exit].route.name_no_abbrev() + "_")
                        or colocated[try_as_match].label.startswith(colocated[try_as_exit].route.name_no_abbrev() + "/")
                        or colocated[try_as_match].label == colocated[try_as_exit].label):
                    this_match = True
                if not this_match:
                    all_match = False

            if all_match:
                label = ""
                for pos in range(len(colocated)):
                    if pos == try_as_exit:
                        label += colocated[pos].route.list_entry_name() + "(" + colocated[pos].label + ")"
                    else:
                        label += colocated[pos].route.list_entry_name()
                    if pos < len(colocated) - 1:
                        label += "/"
                log.append("Exit number: " + name + " -> " + label)
                return label

        # TODO: I-20@76&I-77@16
        # should become I-20/I-77 or maybe I-20(76)/I-77(16)
        # not shorter, so maybe who cares about this one?

        # TODO: US83@FM1263_S&US380@FM1263
        # should probably end up as US83/US280@FM1263 or @FM1263_S

        # How about?
        # I-581@4&US220@I-581(4)&US460@I-581&US11AltRoa@I-581&US220AltRoa@US220_S&VA116@I-581(4)
        # INVESTIGATE: VA262@US11&US11@VA262&VA262@US11_S
        # should be 2 colocated, shows up as 3?

        # TODO: I-610@TX288&I-610@38&TX288@I-610
        # this is the overlap point of a loop

        # TODO: boundaries where order is reversed on colocated points
        # Vt4@FIN/NOR&E75@NOR/FIN&E75@NOR/FIN

        log.append("Keep failsafe: " + name)
        return name

    def simple_waypoint_name(self):
        """Failsafe name for a point, simply the string of route name @
        label, concatenated with & characters for colocated points."""
        if self.colocated is None:
            return self.route.list_entry_name() + "@" + self.label
        long_label = ""
        for w in self.colocated:
            if w.route.system.active_or_preview():
                if long_label != "":
                    long_label += "&"
                long_label += w.route.list_entry_name() + "@" + w.label
        return long_label

    def is_or_colocated_with_active_or_preview(self):
        if self.route.system.active_or_preview():
            return True
        if self.colocated is not None:
            for w in self.colocated:
                if w.route.system.active_or_preview():
                    return True
        return False

    def is_valid(self):
        return self.lat != 0.0 or self.lng != 0.0


class HighwaySegment:
    """This class represents one highway segment: the connection between two
    Waypoints connected by one or more routes"""

    def __init__(self, w1, w2, route):
        self.waypoint1 = w1
        self.waypoint2 = w2
        self.route = route
        self.concurrent = None
        self.clinched_by = set()
        self.segment_name = None

    def __str__(self):
        return self.route.readable_name() + " " + self.waypoint1.label + " " + self.waypoint2.label

    def add_clinched_by(self, traveler):
        if traveler not in self.clinched_by:
            self.clinched_by.add(traveler)
            return True
        else:
            return False

    def csv_line(self, id):
        """return csv line to insert into a table"""
        return "'" + str(id) + "','" + str(self.waypoint1.point_num) + "','" + str(
            self.waypoint2.point_num) + "','" + self.route.root + "'"

    def length(self):
        """return segment length in miles"""
        return self.waypoint1.distance_to(self.waypoint2)

    def set_segment_name(self):
        """compute and set a segment name based on names of all
        concurrent routes, used for graph edge labels"""
        self.segment_name = ""
        if self.concurrent is None:
            if self.route.system.active_or_preview():
                self.segment_name += self.route.list_entry_name()
        else:
            for cs in self.concurrent:
                if cs.route.system.active_or_preview():
                    if self.segment_name != "":
                        self.segment_name += ","
                    self.segment_name += cs.route.list_entry_name()


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
    an instance of HighwaySystem

    Region: the project region or subdivision in which the
    route belongs.

    Route: the route name as would be specified in user lists

    Banner: the (optional) banner on the route such as 'Alt',
    'Bus', or 'Trk'.  Now allowed up to 6 characters

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

    def __init__(self, line, system, el):
        """initialize object from a .csv file line, but do not
        yet read in waypoint file"""
        fields = line.split(";")
        if len(fields) != 8:
            el.add_error("Could not parse csv line: [" + line +
                         "], expected 8 fields, found " + str(len(fields)))
        self.system = system
        if system.systemname != fields[0]:
            el.add_error("System mismatch parsing line [" + line + "], expected " + system.systemname)
        self.region = fields[1]
        self.route = fields[2]
        self.banner = fields[3]
        self.abbrev = fields[4]
        self.city = fields[5].replace("'", "''")
        self.root = fields[6]
        self.alt_route_names = fields[7].split(",")
        self.point_list = []
        self.labels_in_use = set()
        self.unused_alt_labels = set()
        self.segment_list = []
        self.mileage = 0.0
        self.rootOrder = -1  # order within connected route

    def __str__(self):
        """printable version of the object"""
        return self.root + " (" + str(len(self.point_list)) + " total points)"

    def read_wpt(self, all_waypoints, all_waypoints_lock, datacheckerrors, el, path="../../../HighwayData/hwy_data"):
        """read data into the Route's waypoint list from a .wpt file"""
        # print("read_wpt on " + str(self))
        self.point_list = []
        try:
            file = open(path + "/" + self.region + "/" + self.system.systemname + "/" + self.root + ".wpt", "rt",
                        encoding='utf-8')
        except OSError as e:
            el.add_error(str(e))
        else:
            lines = file.readlines()
            file.close()
            w = None
            for line in lines:
                line = line.strip()
                if len(line) > 0:
                    previous_point = w
                    w = Waypoint(line, self, datacheckerrors)
                    if w.is_valid() == False:
                        w = previous_point
                        continue
                    self.point_list.append(w)
                    # populate unused alt labels
                    for label in w.alt_labels:
                        self.unused_alt_labels.add(label.upper().strip("+"))
                    # look for colocated points
                    all_waypoints_lock.acquire()
                    other_w = all_waypoints.waypoint_at_same_point(w)
                    if other_w is not None:
                        # see if this is the first point colocated with other_w
                        if other_w.colocated is None:
                            other_w.colocated = [other_w]
                        other_w.colocated.append(w)
                        w.colocated = other_w.colocated

                    # look for near-miss points (before we add this one in)
                    # print("DEBUG: START search for nmps for waypoint " + str(w) + " in quadtree of size " + str(all_waypoints.size()))
                    # if not all_waypoints.is_valid():
                    #    sys.exit()
                    nmps = all_waypoints.near_miss_waypoints(w, 0.0005)
                    # print("DEBUG: for waypoint " + str(w) + " got " + str(len(nmps)) + " nmps: ", end="")
                    # for dbg_w in nmps:
                    #    print(str(dbg_w) + " ", end="")
                    # print()
                    if len(nmps) > 0:
                        if w.near_miss_points is None:
                            w.near_miss_points = nmps
                        else:
                            w.near_miss_points.extend(nmps)

                        for other_w in nmps:
                            if other_w.near_miss_points is None:
                                other_w.near_miss_points = [w]
                            else:
                                other_w.near_miss_points.append(w)

                    all_waypoints.insert(w)
                    all_waypoints_lock.release()
                    # add HighwaySegment, if not first point
                    if previous_point is not None:
                        self.segment_list.append(HighwaySegment(previous_point, w, self))

    def print_route(self):
        for point in self.point_list:
            print(str(point))

    def find_segment_by_waypoints(self, w1, w2):
        for s in self.segment_list:
            if s.waypoint1 is w1 and s.waypoint2 is w2 or s.waypoint1 is w2 and s.waypoint2 is w1:
                return s
        return None

    def csv_line(self):
        """return csv line to insert into a table"""
        # note: alt_route_names does not need to be in the db since
        # list preprocessing uses alt or canonical and no longer cares
        return "'" + self.system.systemname + "','" + self.region + "','" + self.route + "','" + self.banner + "','" + self.abbrev + "','" + self.city + "','" + self.root + "','" + str(
            self.mileage) + "','" + str(self.rootOrder) + "'";

    def readable_name(self):
        """return a string for a human-readable route name"""
        return self.region + " " + self.route + self.banner + self.abbrev

    def list_entry_name(self):
        """return a string for a human-readable route name in the
        format expected in traveler list files"""
        return self.route + self.banner + self.abbrev

    def name_no_abbrev(self):
        """return a string for a human-readable route name in the
        format that might be encountered for intersecting route
        labels, where the abbrev field is often omitted"""
        return self.route + self.banner

    def clinched_by_traveler(self, t):
        miles = 0.0
        for s in self.segment_list:
            if t in s.clinched_by:
                miles += s.length()
        return miles


class ConnectedRoute:
    """This class encapsulates a single 'connected route' as given
    by a single line of a _con.csv file
    """

    def __init__(self, line, system, el):
        """initialize the object from the _con.csv line given"""
        fields = line.split(";")
        if len(fields) != 5:
            el.add_error("Could not parse _con.csv line: [" + line +
                         "] expected 5 fields, found " + str(len(fields)))
        self.system = system
        if system.systemname != fields[0]:
            el.add_error("System mismatch parsing line [" + line + "], expected " + system.systemname)
        self.route = fields[1]
        self.banner = fields[2]
        self.groupname = fields[3]
        # fields[4] is the list of roots, which will become a python list
        # of Route objects already in the system
        self.roots = []
        roots = fields[4].split(",")
        rootOrder = 0
        for root in roots:
            route = None
            for check_route in system.route_list:
                if check_route.root == root:
                    route = check_route
                    break
            if route is None:
                el.add_error("Could not find Route matching root " + root +
                             " in system " + system.systemname + '.')
            else:
                self.roots.append(route)
                # save order of route in connected route
                route.rootOrder = rootOrder
            rootOrder += 1
        if len(self.roots) < 1:
            el.add_error("No roots in _con.csv line [" + line + "]")
        # will be computed for routes in active & preview systems later
        self.mileage = 0.0

    def csv_line(self):
        """return csv line to insert into a table"""
        return "'" + self.system.systemname + "','" + self.route + "','" + self.banner + "','" + self.groupname.replace(
            "'", "''") + "','" + self.roots[0].root + "','" + str(self.mileage) + "'";

    def readable_name(self):
        """return a string for a human-readable connected route name"""
        ans = self.route + self.banner
        if self.groupname != "":
            ans += " (" + self.groupname + ")"
        return ans


class HighwaySystem:
    """This class encapsulates the contents of one .csv file
    that represents the collection of highways within a system.

    See Route for information about the fields of a .csv file

    With the implementation of three tiers of systems (active,
    preview, devel), added parameter and field here, to be stored in
    DB

    After construction and when all Route entries are made, a _con.csv
    file is read that defines the connected routes in the system.
    In most cases, the connected route is just a single Route, but when
    a designation within the same system crosses region boundaries,
    a connected route defines the entirety of the route.
    """

    def __init__(self, systemname, country, fullname, color, tier, level, el,
                 path="../../../HighwayData/hwy_data/_systems"):
        self.route_list = []
        self.con_route_list = []
        self.systemname = systemname
        self.country = country
        self.fullname = fullname
        self.color = color
        self.tier = tier
        self.level = level
        self.mileage_by_region = dict()
        try:
            file = open(path + "/" + systemname + ".csv", "rt", encoding='utf-8')
        except OSError as e:
            el.add_error(str(e))
        else:
            lines = file.readlines()
            file.close()
            # ignore the first line of field names
            lines.pop(0)
            for line in lines:
                self.route_list.append(Route(line.rstrip('\n'), self, el))
        try:
            file = open(path + "/" + systemname + "_con.csv", "rt", encoding='utf-8')
        except OSError as e:
            el.add_error(str(e))
        else:
            lines = file.readlines()
            file.close()
            # again, ignore first line with field names
            lines.pop(0)
            for line in lines:
                self.con_route_list.append(ConnectedRoute(line.rstrip('\n'),
                                                          self, el))

    """Return whether this is an active system"""

    def active(self):
        return self.level == "active"

    """Return whether this is a preview system"""

    def preview(self):
        return self.level == "preview"

    """Return whether this is an active or preview system"""

    def active_or_preview(self):
        return self.level == "active" or self.level == "preview"

    """Return whether this is a development system"""

    def devel(self):
        return self.level == "devel"

    """String representation"""

    def __str__(self):
        return self.systemname