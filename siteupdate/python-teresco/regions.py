def parse_regions(et, el, args):
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

    return all_regions, countries, continents
