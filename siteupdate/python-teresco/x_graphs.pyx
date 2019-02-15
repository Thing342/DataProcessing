import math

from datachecks import DatacheckEntry


class GraphListEntry:
    """This class encapsulates information about generated graphs for
    inclusion in the DB table.  Field names here match column names
    in the "graphs" DB table.
    """

    def __init__(self,filename,descr,vertices,edges,format,category):
        self.filename = filename
        self.descr = descr
        self.vertices = vertices
        self.edges = edges
        self.format = format
        self.category = category


class HighwayGraphVertexInfo:
    """This class encapsulates information needed for a highway graph
    vertex.
    """

    def __init__(self, waypoint_list, datacheckerrors):
        self.lat = waypoint_list[0].lat
        self.lng = waypoint_list[0].lng
        self.unique_name = waypoint_list[0].unique_name
        # will consider hidden iff all colocated waypoints are hidden
        self.is_hidden = True
        # note: if saving the first waypoint, no longer need first
        # three fields and can replace with methods
        self.first_waypoint = waypoint_list[0]
        self.regions = set()
        self.systems = set()
        for w in waypoint_list:
            if not w.is_hidden:
                self.is_hidden = False
            self.regions.add(w.route.region)
            self.systems.add(w.route.system)
        self.incident_edges = []
        self.incident_collapsed_edges = []
        # VISIBLE_HIDDEN_COLOC datacheck
        if self.visible_hidden_coloc(waypoint_list):
            # determine which route, label, and info to use for this entry asciibetically
            vis_list = []
            hid_list = []
            for w in waypoint_list:
                if w.is_hidden:
                    hid_list.append(w)
                else:
                    vis_list.append(w)
            datacheckerrors.append(DatacheckEntry(vis_list[0].route, [vis_list[0].label], "VISIBLE_HIDDEN_COLOC",
                                                  hid_list[0].route.root + "@" + hid_list[0].label))

    # printable string
    def __str__(self):
        return self.unique_name

    @staticmethod
    def visible_hidden_coloc(waypoint_list):
        for w in range(1, len(waypoint_list)):
            if waypoint_list[w].is_hidden != waypoint_list[0].is_hidden:
                return True
        return False


class HighwayGraphEdgeInfo:
    """This class encapsulates information needed for a 'standard'
    highway graph edge.
    """

    def __init__(self, s, graph):
        # temp debug
        self.written = False
        self.segment_name = s.segment_name
        self.vertex1 = graph.vertices[s.waypoint1.unique_name]
        self.vertex2 = graph.vertices[s.waypoint2.unique_name]
        # assumption: each edge/segment lives within a unique region
        self.region = s.route.region
        # a list of route name/system pairs
        self.route_names_and_systems = []
        if s.concurrent is None:
            self.route_names_and_systems.append((s.route.list_entry_name(), s.route.system))
        else:
            for cs in s.concurrent:
                if cs.route.system.devel():
                    continue
                self.route_names_and_systems.append((cs.route.list_entry_name(), cs.route.system))

        # checks for the very unusual cases where an edge ends up
        # in the system as itself and its "reverse"
        duplicate = False
        for e in graph.vertices[s.waypoint1.unique_name].incident_edges:
            if e.vertex1 == self.vertex2 and e.vertex2 == self.vertex1:
                duplicate = True

        for e in graph.vertices[s.waypoint2.unique_name].incident_edges:
            if e.vertex1 == self.vertex2 and e.vertex2 == self.vertex1:
                duplicate = True

        if not duplicate:
            graph.vertices[s.waypoint1.unique_name].incident_edges.append(self)
            graph.vertices[s.waypoint2.unique_name].incident_edges.append(self)

    # compute an edge label, optionally resticted by systems
    def label(self, systems=None):
        the_label = ""
        for (name, system) in self.route_names_and_systems:
            if systems is None or system in systems:
                if the_label == "":
                    the_label = name
                else:
                    the_label += "," + name

        return the_label

    # printable string for this edge
    def __str__(self):
        return "HighwayGraphEdgeInfo: " + self.segment_name + " from " + str(self.vertex1) + " to " + str(self.vertex2)


class HighwayGraphCollapsedEdgeInfo:
    """This class encapsulates information needed for a highway graph
    edge that can incorporate intermediate points.
    """

    def __init__(self, graph, segment=None, vertex_info=None):
        if segment is None and vertex_info is None:
            print("ERROR: improper use of HighwayGraphCollapsedEdgeInfo constructor\n")
            return

        # a few items we can do for either construction type
        self.written = False

        # intermediate points, if more than 1, will go from vertex1 to
        # vertex2
        self.intermediate_points = []

        # initial construction is based on a HighwaySegment
        if segment is not None:
            self.segment_name = segment.segment_name
            self.vertex1 = graph.vertices[segment.waypoint1.unique_name]
            self.vertex2 = graph.vertices[segment.waypoint2.unique_name]
            # assumption: each edge/segment lives within a unique region
            # and a 'multi-edge' would not be able to span regions as there
            # would be a required visible waypoint at the border
            self.region = segment.route.region
            # a list of route name/system pairs
            self.route_names_and_systems = []
            if segment.concurrent is None:
                self.route_names_and_systems.append((segment.route.list_entry_name(), segment.route.system))
            else:
                for cs in segment.concurrent:
                    if cs.route.system.devel():
                        continue
                    self.route_names_and_systems.append((cs.route.list_entry_name(), cs.route.system))

            # checks for the very unusual cases where an edge ends up
            # in the system as itself and its "reverse"
            duplicate = False
            for e in self.vertex1.incident_collapsed_edges:
                if e.vertex1 == self.vertex2 and e.vertex2 == self.vertex1:
                    duplicate = True

            for e in self.vertex2.incident_collapsed_edges:
                if e.vertex1 == self.vertex2 and e.vertex2 == self.vertex1:
                    duplicate = True

            if not duplicate:
                self.vertex1.incident_collapsed_edges.append(self)
                self.vertex2.incident_collapsed_edges.append(self)

        # build by collapsing two existing edges around a common
        # hidden vertex waypoint, whose information is given in
        # vertex_info
        if vertex_info is not None:
            # we know there are exactly 2 incident edges, as we
            # checked for that, and we will replace these two
            # with the single edge we are constructing here
            edge1 = vertex_info.incident_collapsed_edges[0]
            edge2 = vertex_info.incident_collapsed_edges[1]
            # segment names should match as routes should not start or end
            # nor should concurrencies begin or end at a hidden point
            if edge1.segment_name != edge2.segment_name:
                print(
                    "ERROR: segment name mismatch in HighwayGraphCollapsedEdgeInfo: edge1 named " + edge1.segment_name + " edge2 named " + edge2.segment_name + "\n")
            self.segment_name = edge1.segment_name
            # print("\nDEBUG: collapsing edges along " + self.segment_name + " at vertex " + str(vertex_info) + ", edge1 is " + str(edge1) + " and edge2 is " + str(edge2))
            # region and route names/systems should also match, but not
            # doing that sanity check here, as the above check should take
            # care of that
            self.region = edge1.region
            self.route_names_and_systems = edge1.route_names_and_systems

            # figure out and remember which endpoints are not the
            # vertex we are collapsing and set them as our new
            # endpoints, and at the same time, build up our list of
            # intermediate vertices
            self.intermediate_points = edge1.intermediate_points.copy()
            # print("DEBUG: copied edge1 intermediates" + self.intermediate_point_string())

            if edge1.vertex1 == vertex_info:
                # print("DEBUG: self.vertex1 getting edge1.vertex2: " + str(edge1.vertex2) + " and reversing edge1 intermediates")
                self.vertex1 = edge1.vertex2
                self.intermediate_points.reverse()
            else:
                # print("DEBUG: self.vertex1 getting edge1.vertex1: " + str(edge1.vertex1))
                self.vertex1 = edge1.vertex1

            # print("DEBUG: appending to intermediates: " + str(vertex_info))
            self.intermediate_points.append(vertex_info)

            toappend = edge2.intermediate_points.copy()
            # print("DEBUG: copied edge2 intermediates" + edge2.intermediate_point_string())
            if edge2.vertex1 == vertex_info:
                # print("DEBUG: self.vertex2 getting edge2.vertex2: " + str(edge2.vertex2))
                self.vertex2 = edge2.vertex2
            else:
                # print("DEBUG: self.vertex2 getting edge2.vertex1: " + str(edge2.vertex1) + " and reversing edge2 intermediates")
                self.vertex2 = edge2.vertex1
                toappend.reverse()

            self.intermediate_points.extend(toappend)

            # print("DEBUG: intermediates complete: from " + str(self.vertex1) + " via " + self.intermediate_point_string() + " to " + str(self.vertex2))

            # replace edge references at our endpoints with ourself
            removed = 0
            if edge1 in self.vertex1.incident_collapsed_edges:
                self.vertex1.incident_collapsed_edges.remove(edge1)
                removed += 1
            if edge1 in self.vertex2.incident_collapsed_edges:
                self.vertex2.incident_collapsed_edges.remove(edge1)
                removed += 1
            if removed != 1:
                print("ERROR: edge1 " + str(edge1) + " removed from " + str(removed) + " adjacency lists instead of 1.")
            removed = 0
            if edge2 in self.vertex1.incident_collapsed_edges:
                self.vertex1.incident_collapsed_edges.remove(edge2)
                removed += 1
            if edge2 in self.vertex2.incident_collapsed_edges:
                self.vertex2.incident_collapsed_edges.remove(edge2)
                removed += 1
            if removed != 1:
                print("ERROR: edge2 " + str(edge2) + " removed from " + str(removed) + " adjacency lists instead of 1.")
            self.vertex1.incident_collapsed_edges.append(self)
            self.vertex2.incident_collapsed_edges.append(self)

    # compute an edge label, optionally resticted by systems
    def label(self, systems=None):
        the_label = ""
        for (name, system) in self.route_names_and_systems:
            if systems is None or system in systems:
                if the_label == "":
                    the_label = name
                else:
                    the_label += "," + name

        return the_label

    # printable string for this edge
    def __str__(self):
        return "HighwayGraphCollapsedEdgeInfo: " + self.segment_name + " from " + str(self.vertex1) + " to " + str(
            self.vertex2) + " via " + str(len(self.intermediate_points)) + " points"

    # line appropriate for a tmg collapsed edge file
    def collapsed_tmg_line(self, systems=None):
        line = str(self.vertex1.vis_vertex_num) + " " + str(self.vertex2.vis_vertex_num) + " " + self.label(systems)
        for intermediate in self.intermediate_points:
            line += " " + str(intermediate.lat) + " " + str(intermediate.lng)
        return line

    # line appropriate for a tmg collapsed edge file, with debug info
    def debug_tmg_line(self, systems=None):
        line = str(self.vertex1.vertex_num) + " [" + self.vertex1.unique_name + "] " + str(
            self.vertex2.vertex_num) + " [" + self.vertex2.unique_name + "] " + self.label(systems)
        for intermediate in self.intermediate_points:
            line += " [" + intermediate.unique_name + "] " + str(intermediate.lat) + " " + str(intermediate.lng)
        return line

    # return the intermediate points as a string
    def intermediate_point_string(self):
        if len(self.intermediate_points) == 0:
            return " None"

        line = ""
        for intermediate in self.intermediate_points:
            line += " [" + intermediate.unique_name + "] " + str(intermediate.lat) + " " + str(intermediate.lng)
        return line


class PlaceRadius:
    """This class encapsulates a place name, file base name, latitude,
    longitude, and radius (in miles) to define the area to which our
    place-based graphs are restricted.
    """

    def __init__(self, place, base, lat, lng, r):
        self.place = place
        self.base = base
        self.lat = float(lat)
        self.lng = float(lng)
        self.r = int(r)

    def contains_vertex_info(self, vinfo):
        """return whether vinfo's coordinates are within this area"""
        # convert to radians to compte distance
        rlat1 = math.radians(self.lat)
        rlng1 = math.radians(self.lng)
        rlat2 = math.radians(vinfo.lat)
        rlng2 = math.radians(vinfo.lng)

        ans = math.acos(math.cos(rlat1) * math.cos(rlng1) * math.cos(rlat2) * math.cos(rlng2) + \
                        math.cos(rlat1) * math.sin(rlng1) * math.cos(rlat2) * math.sin(rlng2) + \
                        math.sin(rlat1) * math.sin(rlat2)) * 3963.1  # EARTH_RADIUS;
        return ans <= self.r

    def contains_waypoint(self, w):
        """return whether w is within this area"""
        # convert to radians to compte distance
        rlat1 = math.radians(self.lat)
        rlng1 = math.radians(self.lng)
        rlat2 = math.radians(w.lat)
        rlng2 = math.radians(w.lng)

        ans = math.acos(math.cos(rlat1) * math.cos(rlng1) * math.cos(rlat2) * math.cos(rlng2) + \
                        math.cos(rlat1) * math.sin(rlng1) * math.cos(rlat2) * math.sin(rlng2) + \
                        math.sin(rlat1) * math.sin(rlat2)) * 3963.1  # EARTH_RADIUS;
        return ans <= self.r

    def contains_edge(self, e):
        """return whether both endpoints of edge e are within this area"""
        return (self.contains_waypoint(e.vertex1) and
                self.contains_waypoint(e.vertex2))


class HighwayGraph:
    """This class implements the capability to create graph
    data structures representing the highway data.

    On construction, build a list (as keys of a dict) of unique
    waypoint names that can be used as vertex labels in unique_waypoints,
    and a determine edges, at most one per concurrent segment, by
    setting segment names only on certain HighwaySegments in the overall
    data set.  Create two sets of edges - one for the full graph
    and one for the graph with hidden waypoints compressed into
    multi-point edges.
    """

    def __init__(self, all_waypoints, highway_systems, datacheckerrors):
        # first, build a list of the unique waypoints and create
        # unique names that will be our vertex labels, these will
        # be in a dict where the keys are the unique vertex labels
        # and the values are lists of Waypoint objects (either a
        # colocation list or a singleton list for a place occupied
        # by a single Waypoint)
        self.unique_waypoints = dict()
        all_waypoint_list = all_waypoints.point_list()
        self.highway_systems = highway_systems

        # add a unique name field to each waypoint, initialized to
        # None, which should get filled in later for any waypoint that
        # is or shares a location with any waypoint in an active or
        # preview system
        for w in all_waypoint_list:
            w.unique_name = None

        # to track the waypoint name compressions, add log entries
        # to this list
        self.waypoint_naming_log = []

        # loop again, for each Waypoint, create a unique name and an
        # entry in the unique_waypoints list, unless it's a point not
        # in or colocated with any active or preview system
        for w in all_waypoint_list:
            # skip if this point is occupied by only waypoints in
            # devel systems
            if not w.is_or_colocated_with_active_or_preview():
                continue

            # skip if named previously as someone else's colocated point
            if w.unique_name is not None:
                continue

            # come up with a unique name that brings in its meaning

            # start with the canonical name
            point_name = w.canonical_waypoint_name(self.waypoint_naming_log)

            # if that's taken, append the region code
            if point_name in self.unique_waypoints:
                point_name += "|" + w.route.region
                self.waypoint_naming_log.append("Appended region: " + point_name)

            # if that's taken, see if the simple name
            # is available
            if point_name in self.unique_waypoints:
                simple_name = w.simple_waypoint_name()
                if simple_name not in self.unique_waypoints:
                    self.waypoint_naming_log.append("Revert to simple: " + simple_name + " from (taken) " + point_name)
                    point_name = simple_name

            # if we have not yet succeeded, add !'s until we do
            while point_name in self.unique_waypoints:
                point_name += "!"
                self.waypoint_naming_log.append("Appended !: " + point_name)

            # we're good, add the list of waypoints, either as a
            # singleton list or the colocated list a values for the
            # key of the unique name we just computed
            if w.colocated is None:
                self.unique_waypoints[point_name] = [w]
            else:
                self.unique_waypoints[point_name] = w.colocated

            # mark each of these Waypoint objects also with this name
            for wpt in self.unique_waypoints[point_name]:
                wpt.unique_name = point_name

        # now create graph edges from highway segments start by
        # marking all as unvisited and giving a segment name of None,
        # so only those segments that are given a name are used later
        # in the graph edge listing
        for h in highway_systems:
            if h.devel():
                continue
            for r in h.route_list:
                for s in r.segment_list:
                    s.visited = False
                    s.segment_name = None

        # now go back and visit again, but concurrent segments just
        # get named once. also count up unique edges as we go
        self.unique_edges = 0
        for h in highway_systems:
            if h.devel():
                continue
            for r in h.route_list:
                for s in r.segment_list:
                    if not s.visited:
                        self.unique_edges += 1
                        s.set_segment_name()
                        if s.concurrent is None:
                            s.visited = True
                        else:
                            for cs in s.concurrent:
                                cs.visited = True

        # Full graph info now complete.  Next, build a graph structure
        # that is more convenient to use.

        # One copy of the vertices
        self.vertices = {}
        for label, pointlist in self.unique_waypoints.items():
            self.vertices[label] = HighwayGraphVertexInfo(pointlist, datacheckerrors)

        # add edges, which end up in vertex adjacency lists, first one
        # copy for the full graph
        for h in self.highway_systems:
            if h.devel():
                continue
            for r in h.route_list:
                for s in r.segment_list:
                    if s.segment_name is not None:
                        HighwayGraphEdgeInfo(s, self)

        print("Full graph has " + str(len(self.vertices)) +
              " vertices, " + str(self.edge_count()) + " edges.")

        # add edges again, which end up in a separate set of vertex
        # adjacency lists, this one will be used to create a graph
        # where the hidden waypoints are merged into the edge
        # structures
        for h in self.highway_systems:
            if h.devel():
                continue
            for r in h.route_list:
                for s in r.segment_list:
                    if s.segment_name is not None:
                        HighwayGraphCollapsedEdgeInfo(self, segment=s)

        # compress edges adjacent to hidden vertices
        for label, vinfo in self.vertices.items():
            if vinfo.is_hidden:
                if len(vinfo.incident_collapsed_edges) < 2:
                    # these cases are flagged as HIDDEN_TERMINUS
                    vinfo.is_hidden = False
                    continue
                if len(vinfo.incident_collapsed_edges) > 2:
                    datacheckerrors.append(DatacheckEntry(vinfo.first_waypoint.colocated[0].route,
                                                          [vinfo.first_waypoint.colocated[0].label],
                                                          "HIDDEN_JUNCTION", str(len(vinfo.incident_collapsed_edges))))
                    vinfo.is_hidden = False
                    continue
                # construct from vertex_info this time
                HighwayGraphCollapsedEdgeInfo(self, vertex_info=vinfo)

        # print summary info
        print("Edge compressed graph has " + str(self.num_visible_vertices()) +
              " vertices, " + str(self.collapsed_edge_count()) + " edges.")

    def num_visible_vertices(self):
        count = 0
        for v in self.vertices.values():
            if not v.is_hidden:
                count += 1
        return count

    def edge_count(self):
        edges = 0
        for v in self.vertices.values():
            edges += len(v.incident_edges)
        return edges // 2

    def collapsed_edge_count(self):
        edges = 0
        for v in self.vertices.values():
            if not v.is_hidden:
                edges += len(v.incident_collapsed_edges)
        return edges // 2

    def matching_vertices(self, regions, systems, placeradius):
        # return a list of vertices from the graph, optionally
        # restricted by region or system or placeradius area
        vis = 0
        vertex_list = []
        for vinfo in self.vertices.values():
            if placeradius is not None and not placeradius.contains_vertex_info(vinfo):
                continue
            region_match = regions is None
            if not region_match:
                for r in regions:
                    if r in vinfo.regions:
                        region_match = True
                        break
            if not region_match:
                continue
            system_match = systems is None
            if not system_match:
                for s in systems:
                    if s in vinfo.systems:
                        system_match = True
                        break
            if not system_match:
                continue
            if not vinfo.is_hidden:
                vis += 1
            vertex_list.append(vinfo)
        return (vertex_list, vis)

    def matching_edges(self, mv, regions=None, systems=None, placeradius=None):
        # return a set of edges from the graph, optionally
        # restricted by region or system or placeradius area
        edge_set = set()
        for v in mv:
            for e in v.incident_edges:
                if placeradius is None or placeradius.contains_edge(e):
                    if regions is None or e.region in regions:
                        system_match = systems is None
                        if not system_match:
                            for (r, s) in e.route_names_and_systems:
                                if s in systems:
                                    system_match = True
                        if system_match:
                            edge_set.add(e)
        return edge_set

    def matching_collapsed_edges(self, mv, regions=None, systems=None,
                                 placeradius=None):
        # return a set of edges from the graph edges for the collapsed
        # edge format, optionally restricted by region or system or
        # placeradius area
        edge_set = set()
        for v in mv:
            if v.is_hidden:
                continue
            for e in v.incident_collapsed_edges:
                if placeradius is None or placeradius.contains_edge(e):
                    if regions is None or e.region in regions:
                        system_match = systems is None
                        if not system_match:
                            for (r, s) in e.route_names_and_systems:
                                if s in systems:
                                    system_match = True
                        if system_match:
                            edge_set.add(e)
        return edge_set

    # write the entire set of highway data a format very similar to
    # the original .gra format.  The first line is a header specifying
    # the format and version number, the second line specifying the
    # number of waypoints, w, and the number of connections, c, then w
    # lines describing waypoints (label, latitude, longitude), then c
    # lines describing connections (endpoint 1 number, endpoint 2
    # number, route label)
    #
    # returns tuple of number of vertices and number of edges written
    #
    def write_master_tmg_simple(self, filename):
        tmgfile = open(filename, 'w')
        tmgfile.write("TMG 1.0 simple\n")
        tmgfile.write(str(len(self.vertices)) + ' ' + str(self.edge_count()) + '\n')
        # number waypoint entries as we go to support original .gra
        # format output
        vertex_num = 0
        for label, vinfo in self.vertices.items():
            tmgfile.write(label + ' ' + str(vinfo.lat) + ' ' + str(vinfo.lng) + '\n')
            vinfo.vertex_num = vertex_num
            vertex_num += 1

        # sanity check
        if len(self.vertices) != vertex_num:
            print("ERROR: computed " + str(len(self.vertices)) + " waypoints but wrote " + str(vertex_num))

        # now edges, only print if not already printed
        edge = 0
        for v in self.vertices.values():
            for e in v.incident_edges:
                if not e.written:
                    e.written = True
                    tmgfile.write(str(e.vertex1.vertex_num) + ' ' + str(e.vertex2.vertex_num) + ' ' + e.label() + '\n')
                    edge += 1

        # sanity checks
        for v in self.vertices.values():
            for e in v.incident_edges:
                if not e.written:
                    print("ERROR: never wrote edge " + str(e.vertex1.vertex_num) + ' ' + str(
                        e.vertex2.vertex_num) + ' ' + e.label() + '\n')
        if self.edge_count() != edge:
            print("ERROR: computed " + str(self.edge_count()) + " edges but wrote " + str(edge) + "\n")

        tmgfile.close()
        return (len(self.vertices), self.edge_count())

    # write the entire set of data in the tmg collapsed edge format
    def write_master_tmg_collapsed(self, filename):
        tmgfile = open(filename, 'w')
        tmgfile.write("TMG 1.0 collapsed\n")
        #print("(" + str(self.num_visible_vertices()) + "," + str(self.collapsed_edge_count()) + ") ", end="", flush=True)
        tmgfile.write(str(self.num_visible_vertices()) + " " +
                      str(self.collapsed_edge_count()) + "\n")

        # write visible vertices
        vis_vertex_num = 0
        for label, vinfo in self.vertices.items():
            if not vinfo.is_hidden:
                vinfo.vis_vertex_num = vis_vertex_num
                tmgfile.write(label + ' ' + str(vinfo.lat) + ' ' + str(vinfo.lng) + '\n')
                vis_vertex_num += 1

        # write collapsed edges
        edge = 0
        for v in self.vertices.values():
            if not v.is_hidden:
                for e in v.incident_collapsed_edges:
                    if not e.written:
                        e.written = True
                        tmgfile.write(e.collapsed_tmg_line() + '\n')
                        edge += 1

        # sanity check on edges written
        if self.collapsed_edge_count() != edge:
            print("ERROR: computed " + str(self.collapsed_edge_count()) + " collapsed edges, but wrote " + str(
                edge) + "\n")

        tmgfile.close()
        return (self.num_visible_vertices(), self.collapsed_edge_count())

    # write a subset of the data,
    # in both simple and collapsed formats,
    # restricted by regions in the list if given,
    # by system in the list if given,
    # or to within a given area if placeradius is given
    def write_subgraphs_tmg(self, graph_list, path, root, descr, category, regions, systems, placeradius):
        visible = 0
        simplefile = open(path + root + "-simple.tmg", "w", encoding='utf-8')
        collapfile = open(path + root + ".tmg", "w", encoding='utf-8')
        (mv, visible) = self.matching_vertices(regions, systems, placeradius)
        mse = self.matching_edges(mv, regions, systems, placeradius)
        mce = self.matching_collapsed_edges(mv, regions, systems, placeradius)
        #print('(' + str(len(mv)) + ',' + str(len(mse)) + ") ", end="", flush=True)
        #print('(' + str(visible) + ',' + str(len(mce)) + ") ", end="", flush=True)
        simplefile.write("TMG 1.0 simple\n")
        collapfile.write("TMG 1.0 collapsed\n")
        simplefile.write(str(len(mv)) + ' ' + str(len(mse)) + '\n')
        collapfile.write(str(visible) + ' ' + str(len(mce)) + '\n')

        # write vertices
        sv = 0
        cv = 0
        for v in mv:
            # all vertices, for simple graph
            simplefile.write(v.unique_name + ' ' + str(v.lat) + ' ' + str(v.lng) + '\n')
            v.vertex_num = sv
            sv += 1
            # visible vertices, for collapsed graph
            if not v.is_hidden:
                collapfile.write(v.unique_name + ' ' + str(v.lat) + ' ' + str(v.lng) + '\n')
                v.vis_vertex_num = cv
                cv += 1
        # write edges
        for e in mse:
            simplefile.write(
                str(e.vertex1.vertex_num) + ' ' + str(e.vertex2.vertex_num) + ' ' + e.label(systems) + '\n')
        for e in mce:
            collapfile.write(e.collapsed_tmg_line(systems) + '\n')
        simplefile.close()
        collapfile.close()

        graph_list.append(GraphListEntry(root + "-simple.tmg", descr, len(mv), len(mse), "simple", category))
        graph_list.append(GraphListEntry(root + ".tmg", descr, visible, len(mce), "collapsed", category))