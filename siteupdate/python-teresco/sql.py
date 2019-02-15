def do_write_sql(args,
                 countries,
                 continents,
                 all_regions,
                 highway_systems,
                 traveler_lists,
                 updates,
                 systemupdates,
                 stats,
                 datacheckerrors,
                 graph_types,
                 graph_list):

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

    # Table of all HighwaySegmentstats.
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
    for region in list(stats.active_preview_mileage_by_region.keys()):
        if not first:
            sqlfile.write(",")
        first = False
        active_only_mileage = 0.0
        active_preview_mileage = 0.0
        if region in list(stats.active_only_mileage_by_region.keys()):
            active_only_mileage = stats.active_only_mileage_by_region[region]
        if region in list(stats.active_preview_mileage_by_region.keys()):
            active_preview_mileage = stats.active_preview_mileage_by_region[region]
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
    for line in stats.csmbr_values:
        if not first:
            sqlfile.write(",")
        first = False
        sqlfile.write(line + "\n")
    sqlfile.write(";\n")

    # clinched mileage by connected route, active systems and preview
    # systems only
    sqlfile.write(
        'CREATE TABLE clinchedConnectedRoutes (route VARCHAR(32), traveler VARCHAR(48), mileage FLOAT, clinched BOOLEAN, FOREIGN KEY (route) REFERENCES connectedRoutes(firstRoot));\n')
    for start in range(0, len(stats.ccr_values), 10000):
        sqlfile.write('INSERT INTO clinchedConnectedRoutes VALUES\n')
        first = True
        for line in stats.ccr_values[start:start + 10000]:
            if not first:
                sqlfile.write(",")
            first = False
            sqlfile.write(line + "\n")
        sqlfile.write(";\n")

    # clinched mileage by route, active systems and preview systems only
    sqlfile.write(
        'CREATE TABLE clinchedRoutes (route VARCHAR(32), traveler VARCHAR(48), mileage FLOAT, clinched BOOLEAN, FOREIGN KEY (route) REFERENCES routes(root));\n')
    for start in range(0, len(stats.cr_values), 10000):
        sqlfile.write('INSERT INTO clinchedRoutes VALUES\n')
        first = True
        for line in stats.cr_values[start:start + 10000]:
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
