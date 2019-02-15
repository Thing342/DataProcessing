import datetime
import math
from typing import Dict, List

from util import format_clinched_mi


class StatsTables:
    active_only_mileage_by_region: Dict[str, float] = dict()
    active_preview_mileage_by_region: Dict[str, float] = dict()
    overall_mileage_by_region: Dict[str, float] = dict()

    csmbr_values: List[str] = []
    ccr_values: List[str] = []
    cr_values: List[str] = []


def do_compute_stats(et, args, highway_systems, traveler_lists) -> StatsTables:
    # compute lots of stats, first total mileage by route, system, overall, where
    # system and overall are stored in dictionaries by region
    print(et.et() + "Computing stats.", end="", flush=True)
    # now also keeping separate totals for active only, active+preview,
    # and all for overall (not needed for system, as a system falls into just
    # one of these categories)

    results = StatsTables()

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
                if r.region in results.overall_mileage_by_region:
                    results.overall_mileage_by_region[r.region] = results.overall_mileage_by_region[r.region] + \
                                                          segment_length / overall_concurrency_count
                else:
                    results.overall_mileage_by_region[r.region] = segment_length / overall_concurrency_count

                # next, same thing for active_preview mileage for the region,
                # if active or preview
                if r.system.active_or_preview():
                    if r.region in results.active_preview_mileage_by_region:
                        results.active_preview_mileage_by_region[r.region] = results.active_preview_mileage_by_region[r.region] + \
                                                                     segment_length / active_preview_concurrency_count
                    else:
                        results.active_preview_mileage_by_region[r.region] = segment_length / active_preview_concurrency_count

                # now same thing for active_only mileage for the region,
                # if active
                if r.system.active():
                    if r.region in results.active_only_mileage_by_region:
                        results.active_only_mileage_by_region[r.region] = results.active_only_mileage_by_region[r.region] + \
                                                                  segment_length / active_only_concurrency_count
                    else:
                        results.active_only_mileage_by_region[r.region] = segment_length / active_only_concurrency_count

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
    active_only_miles = math.fsum(list(results.active_only_mileage_by_region.values()))
    hdstatsfile.write("Active routes (active): " + "{0:.2f}".format(active_only_miles) + " mi\n")
    active_preview_miles = math.fsum(list(results.active_preview_mileage_by_region.values()))
    hdstatsfile.write("Clinchable routes (active, preview): " + "{0:.2f}".format(active_preview_miles) + " mi\n")
    overall_miles = math.fsum(list(results.overall_mileage_by_region.values()))
    hdstatsfile.write("All routes (active, preview, devel): " + "{0:.2f}".format(overall_miles) + " mi\n")
    hdstatsfile.write("Breakdown by region:\n")
    # let's sort alphabetically by region instead of using whatever order
    # comes out of the dictionary
    # a nice enhancement later here might break down by continent, then country,
    # then region
    region_entries = []
    for region in list(results.overall_mileage_by_region.keys()):
        # look up active+preview and active-only mileages if they exist
        if region in list(results.active_preview_mileage_by_region.keys()):
            region_active_preview_miles = results.active_preview_mileage_by_region[region]
        else:
            region_active_preview_miles = 0.0
        if region in list(results.active_only_mileage_by_region.keys()):
            region_active_only_miles = results.active_only_mileage_by_region[region]
        else:
            region_active_only_miles = 0.0

        region_entries.append(region + ": " +
                              "{0:.2f}".format(region_active_only_miles) + " (active), " +
                              "{0:.2f}".format(region_active_preview_miles) + " (active, preview) " +
                              "{0:.2f}".format(results.overall_mileage_by_region[region]) + " (active, preview, devel)\n")
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
                total_active_miles = results.active_only_mileage_by_region[region]
            t.log_entries.append(region + ": " +
                                 format_clinched_mi(t_active_miles, total_active_miles) +
                                 ", " +
                                 format_clinched_mi(t.active_preview_mileage_by_region[region],
                                                    results.active_preview_mileage_by_region[region]))

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
    regions = sorted(results.active_only_mileage_by_region.keys())
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
    allfile.write('TOTAL,{0:.2f}'.format(math.fsum(list(results.active_only_mileage_by_region.values()))))
    for region in regions:
        allfile.write(',{0:.2f}'.format(results.active_only_mileage_by_region[region]))
    allfile.write('\n')
    allfile.close()

    # active+preview
    allfile = open(args.csvstatfilepath + "/allbyregionactivepreview.csv", "w", encoding='UTF-8')
    allfile.write("Traveler,Total")
    regions = sorted(results.active_preview_mileage_by_region.keys())
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
    allfile.write('TOTAL,{0:.2f}'.format(math.fsum(list(results.active_preview_mileage_by_region.values()))))
    for region in regions:
        allfile.write(',{0:.2f}'.format(results.active_preview_mileage_by_region[region]))
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

    return results
