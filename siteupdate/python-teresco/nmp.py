import os


def do_nmp(args, et, all_waypoints, highway_systems):
    # read in fp file
    nmpfplist = []
    nmpfpfile = open(args.highwaydatapath + '/nmpfps.log', 'r')
    nmpfpfilelines = nmpfpfile.readlines()
    for line in nmpfpfilelines:
        if len(line.rstrip('\n ')) > 0:
            nmpfplist.append(line.rstrip('\n '))
    nmpfpfile.close()

    nmploglines = []
    nmplog = open(args.logfilepath + '/nearmisspoints.log', 'w')
    nmpnmp = open(args.logfilepath + '/tm-master.nmp', 'w')
    for w in all_waypoints.point_list():
        if w.near_miss_points is not None:
            nmpline = str(w) + " NMP "
            nmplooksintentional = False
            nmpnmplines = []
            # sort the near miss points for consistent ordering to facilitate
            # NMP FP marking
            for other_w in sorted(w.near_miss_points,
                                  key=lambda waypoint:
                                  waypoint.route.root + "@" + waypoint.label):
                if (abs(w.lat - other_w.lat) < 0.0000015) and \
                        (abs(w.lng - other_w.lng) < 0.0000015):
                    nmplooksintentional = True
                nmpline += str(other_w) + " "
                w_label = w.route.root + "@" + w.label
                other_label = other_w.route.root + "@" + other_w.label
                # make sure we only plot once, since the NMP should be listed
                # both ways (other_w in w's list, w in other_w's list)
                if w_label < other_label:
                    nmpnmplines.append(w_label + " " + str(w.lat) + " " + str(w.lng))
                    nmpnmplines.append(other_label + " " + str(other_w.lat) + " " + str(other_w.lng))
            # indicate if this was in the FP list or if it's off by exact amt
            # so looks like it's intentional, and detach near_miss_points list
            # so it doesn't get a rewrite in nmp_merged WPT files
            # also set the extra field to mark FP/LI items in the .nmp file
            extra_field = ""
            if nmpline.rstrip() in nmpfplist:
                nmpfplist.remove(nmpline.rstrip())
                nmpline += "[MARKED FP]"
                w.near_miss_points = None
                extra_field += "FP"
            if nmplooksintentional:
                nmpline += "[LOOKS INTENTIONAL]"
                w.near_miss_points = None
                extra_field += "LI"
            if extra_field != "":
                extra_field = " " + extra_field
            nmploglines.append(nmpline.rstrip())

            # write actual lines to .nmp file, indicating FP and/or LI
            # for marked FPs or looks intentional items
            for nmpnmpline in nmpnmplines:
                nmpnmp.write(nmpnmpline + extra_field + "\n")
    nmpnmp.close()

    # sort and write actual lines to nearmisspoints.log
    nmploglines.sort()
    for n in nmploglines:
        nmplog.write(n + '\n')
    nmploglines = None
    nmplog.close()

    # report any unmatched nmpfps.log entries
    nmpfpsunmatchedfile = open(args.logfilepath + '/nmpfpsunmatched.log', 'w')
    for line in nmpfplist:
        nmpfpsunmatchedfile.write(line + '\n')
    nmpfpsunmatchedfile.close()

    # if requested, rewrite data with near-miss points merged in
    if args.nmpmergepath != "" and not args.errorcheck:
        print(et.et() + "Writing near-miss point merged wpt files.", flush=True)
        for h in highway_systems:
            print(h.systemname, end="", flush=True)
            for r in h.route_list:
                wptpath = args.nmpmergepath + "/" + r.region + "/" + h.systemname
                os.makedirs(wptpath, exist_ok=True)
                wptfile = open(wptpath + "/" + r.root + ".wpt", "wt")
                for w in r.point_list:
                    wptfile.write(w.label + ' ')
                    for a in w.alt_labels:
                        wptfile.write(a + ' ')
                    if w.near_miss_points is None:
                        wptfile.write(
                            "http://www.openstreetmap.org/?lat={0:.6f}".format(w.lat) + "&lon={0:.6f}".format(
                                w.lng) + "\n")
                    else:
                        # for now, arbitrarily choose the northernmost
                        # latitude and easternmost longitude values in the
                        # list and denote a "merged" point with the https
                        lat = w.lat
                        lng = w.lng
                        for other_w in w.near_miss_points:
                            if other_w.lat > lat:
                                lat = other_w.lat
                            if other_w.lng > lng:
                                lng = other_w.lng
                        wptfile.write(
                            "https://www.openstreetmap.org/?lat={0:.6f}".format(lat) + "&lon={0:.6f}".format(
                                lng) + "\n")

                wptfile.close()
            print(".", end="", flush=True)
        print()
