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
        self.unique_locations = 0

    def refine(self):
        """refine a quadtree into 4 sub-quadrants"""
        #print("QTDEBUG: " + str(self) + " being refined")
        self.nw_child = WaypointQuadtree(self.mid_lat,self.min_lng,self.max_lat,self.mid_lng)
        self.ne_child = WaypointQuadtree(self.mid_lat,self.mid_lng,self.max_lat,self.max_lng)
        self.sw_child = WaypointQuadtree(self.min_lat,self.min_lng,self.mid_lat,self.mid_lng)
        self.se_child = WaypointQuadtree(self.min_lat,self.mid_lng,self.mid_lat,self.max_lng)
        points = self.points
        self.points = None
        for p in points:
            self.insert(p)

    def insert(self,w):
        """insert Waypoint w into this quadtree node"""
        #print("QTDEBUG: " + str(self) + " insert " + str(w))
        if self.points is not None:
            if self.waypoint_at_same_point(w) is None:
                #print("QTDEBUG: " + str(self) + " at " + str(self.unique_locations) + " unique locations")
                self.unique_locations += 1
            self.points.append(w)
            if self.unique_locations > 50:  # 50 unique points max per quadtree node
                self.refine()
        else:
            if w.lat < self.mid_lat:
                if w.lng < self.mid_lng:
                    self.sw_child.insert(w)
                else:
                    self.se_child.insert(w)
            else:
                if w.lng < self.mid_lng:
                    self.nw_child.insert(w)
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
                    return self.se_child.waypoint_at_same_point(w)
            else:
                if w.lng < self.mid_lng:
                    return self.nw_child.waypoint_at_same_point(w)
                else:
                    return self.ne_child.waypoint_at_same_point(w)

    def near_miss_waypoints(self, w, tolerance):
        """compute and return a list of existing waypoints which are
        within the near-miss tolerance (in degrees lat, lng) of w"""
        near_miss_points = []

        #print("DEBUG: computing nmps for " + str(w) + " within " + str(tolerance) + " in " + str(self))
        # first check if this is a terminal quadrant, and if it is,
        # we search for NMPs within this quadrant
        if self.points is not None:
            #print("DEBUG: terminal quadrant (self.points is not None) comparing with " + str(len(self.points)) + " points.")
            for p in self.points:
                if p != w and not p.same_coords(w) and p.nearby(w, tolerance):
                    #print("DEBUG: found nmp " + str(p))
                    near_miss_points.append(p)

        # if we're not a terminal quadrant, we need to determine which
        # of our child quadrants we need to search and recurse into
        # each
        else:
            #print("DEBUG: recursive case, mid_lat=" + str(self.mid_lat) + " mid_lng=" + str(self.mid_lng))
            look_north = (w.lat + tolerance) >= self.mid_lat
            look_south = (w.lat - tolerance) <= self.mid_lat
            look_east = (w.lng + tolerance) >= self.mid_lng
            look_west = (w.lng - tolerance) <= self.mid_lng
            #print("DEBUG: recursive case, " + str(look_north) + " " + str(look_south) + " " + str(look_east) + " " + str(look_west))
            # now look in the appropriate child quadrants
            if look_north and look_west:
                near_miss_points.extend(self.nw_child.near_miss_waypoints(w, tolerance))
            if look_north and look_east:
                near_miss_points.extend(self.ne_child.near_miss_waypoints(w, tolerance))
            if look_south and look_west:
                near_miss_points.extend(self.sw_child.near_miss_waypoints(w, tolerance))
            if look_south and look_east:
                near_miss_points.extend(self.se_child.near_miss_waypoints(w, tolerance))

        return near_miss_points

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

    def is_valid(self):
        """make sure the quadtree is valid"""
        if self.points is None:
            # this should be a refined, so should have all 4 children
            if self.nw_child is None:
                print("ERROR: WaypointQuadtree.is_valid refined quadrant has no NW child.")
                return False
            if self.ne_child is None:
                print("ERROR: WaypointQuadtree.is_valid refined quadrant has no NE child.")
                return False
            if self.sw_child is None:
                print("ERROR: WaypointQuadtree.is_valid refined quadrant has no SW child.")
                return False
            if self.se_child is None:
                print("ERROR: WaypointQuadtree.is_valid refined quadrant has no SE child.")
                return False
            return self.nw_child.is_valid() and self.ne_child.is_valid() and self.sw_child.is_valid() and self.se_child.is_valid()

        else:
            # not refined, but should have no more than 50 points
            if self.unique_locations > 50:
                print("ERROR: WaypointQuadtree.is_valid terminal quadrant has too many unique points (" + str(self.unique_locations) + ")")
                return False
            # not refined, so should not have any children
            if self.nw_child is not None:
                print("ERROR: WaypointQuadtree.is_valid terminal quadrant has NW child.")
                return False
            if self.ne_child is not None:
                print("ERROR: WaypointQuadtree.is_valid terminal quadrant has NE child.")
                return False
            if self.sw_child is not None:
                print("ERROR: WaypointQuadtree.is_valid terminal quadrant has SW child.")
                return False
            if self.se_child is not None:
                print("ERROR: WaypointQuadtree.is_valid terminal quadrant has SE child.")
                return False

        return True

    def max_colocated(self):
        """return the maximum number of waypoints colocated at any one location"""
        max_col = 1
        for p in self.point_list():
            if max_col < p.num_colocated():
                max_col = p.num_colocated()
        print("Largest colocate count = " + str(max_col))
        return max_col

    def total_nodes(self):
        if self.points is not None:
            # not refined, no children, return 1 for self
            return 1
        else:
            return 1 + self.nw_child.total_nodes() + self.ne_child.total_nodes() + self.sw_child.total_nodes() + self.se_child.total_nodes()

    def sort(self):
        if self.points is None:
            self.ne_child.sort()
            self.nw_child.sort()
            self.se_child.sort()
            self.sw_child.sort()
        else:
            self.points.sort(key=lambda waypoint: waypoint.route.root + "@" + waypoint.label)
