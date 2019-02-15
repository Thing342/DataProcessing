class DatacheckEntry:
    """This class encapsulates a datacheck log entry

    route is a reference to the route with a datacheck error

    labels is a list of labels that are related to the error (such
    as the endpoints of a too-long segment or the three points that
    form a sharp angle)

    code is the error code string, one of SHARP_ANGLE, BAD_ANGLE,
    DUPLICATE_LABEL, DUPLICATE_COORDS, LABEL_SELFREF,
    LABEL_INVALID_CHAR, LONG_SEGMENT, MALFORMED_URL,
    LABEL_UNDERSCORES, VISIBLE_DISTANCE, LABEL_PARENS, LACKS_GENERIC,
    BUS_WITH_I, NONTERMINAL_UNDERSCORE,
    LONG_UNDERSCORE, LABEL_SLASHES, US_BANNER, VISIBLE_HIDDEN_COLOC,
    HIDDEN_JUNCTION, LABEL_LOOKS_HIDDEN, HIDDEN_TERMINUS,
    OUT_OF_BOUNDS

    info is additional information, at this time either a distance (in
    miles) for a long segment error, an angle (in degrees) for a sharp
    angle error, or a coordinate pair for duplicate coordinates, other
    route/label for point pair errors

    fp is a boolean indicating whether this has been reported as a
    false positive (would be set to true later)

    """

    def __init__(self,route,labels,code,info=""):
         self.route = route
         self.labels = labels
         self.code = code
         self.info = info
         self.fp = False

    def match_except_info(self,fpentry):
        """Check if the fpentry from the csv file matches in all fields
        except the info field"""
        # quick and easy checks first
        if self.route.root != fpentry[0] or self.code != fpentry[4]:
            return False
        # now label matches
        if len(self.labels) > 0 and self.labels[0] != fpentry[1]:
            return False
        if len(self.labels) > 1 and self.labels[1] != fpentry[2]:
            return False
        if len(self.labels) > 2 and self.labels[2] != fpentry[3]:
            return False
        return True

    def __str__(self):
        entry = str(self.route.root)+";"
        if len(self.labels) == 0:
            entry += ";;;"
        elif len(self.labels) == 1:
            entry += self.labels[0]+";;;"
        elif len(self.labels) == 2:
            entry += self.labels[0]+";"+self.labels[1]+";;"
        else:
            entry += self.labels[0]+";"+self.labels[1]+";"+self.labels[2]+";"
        entry += self.code+";"+self.info
        return entry