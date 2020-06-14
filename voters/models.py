from django.db import models

SUFFIXES = {
    'II': 'II', 'III': 'III', 'IV': 'IV', 'V': 'V', 'VI': 'VI',
    'JR': 'Jr.', 'JR.': 'Jr.', 'SR': 'Sr.', 'SR.': 'Sr.',
    'ESQ': 'Esq.', 'ESQ.': 'Esq.',
    '1ST': '1st', '2ND': '2nd', '3RD': '3rd', '3D': '3rd',
    '4TH': '4th', '5TH': '5th', '6TH': '6th',
}
COUNTY_MAP = {
    "ALA": "Alachua",
    "BAK": "Baker",
    "BAY": "Bay",
    "BRA": "Bradford",
    "BRE": "Brevard",
    "BRO": "Broward",
    "CAL": "Calhoun",
    "CHA": "Charlotte",
    "CIT": "Citrus",
    "CLA": "Clay",
    "CLL": "Collier",
    "CLM": "Columbia",
    "DAD": "Miami-Dade",
    "DES": "Desoto",
    "DIX": "Dixie",
    "DUV": "Duval",
    "ESC": "Escambia",
    "FLA": "Flagler",
    "FRA": "Franklin",
    "GAD": "Gadsden",
    "GIL": "Gilchrist",
    "GLA": "Glades",
    "GUL": "Gulf",
    "HAM": "Hamilton",
    "HAR": "Hardee",
    "HEN": "Hendry",
    "HER": "Hernando",
    "HIG": "Highlands",
    "HIL": "Hillsborough",
    "HOL": "Holmes",
    "IND": "Indian River",
    "JAC": "Jackson",
    "JEF": "Jefferson",
    "LAF": "Lafayette",
    "LAK": "Lake",
    "LEE": "Lee",
    "LEO": "Leon",
    "LEV": "Levy",
    "LIB": "Liberty",
    "MAD": "Madison",
    "MAN": "Manatee",
    "MRN": "Marion",
    "MRT": "Martin",
    "MON": "Monroe",
    "NAS": "Nassau",
    "OKA": "Okaloosa",
    "OKE": "Okeechobee",
    "ORA": "Orange",
    "OSC": "Osceola",
    "PAL": "PalmBeach",
    "PAS": "Pasco",
    "PIN": "Pinellas",
    "POL": "Polk",
    "PUT": "Putnam",
    "SAN": "SantaRosa",
    "SAR": "Sarasota",
    "SEM": "Seminole",
    "STJ": "St. Johns",
    "STL": "St. Lucie",
    "SUM": "Sumter",
    "SUW": "Suwannee",
    "TAY": "Taylor",
    "UNI": "Union",
    "VOL": "Volusia",
    "WAK": "Wakulla",
    "WAL": "Walton",
    "WAS": "Washington"
}


class Voter(models.Model):
    lname = models.CharField(max_length=255, blank=True, null=True)
    fname = models.CharField(max_length=255, blank=True, null=True)
    mname = models.CharField(max_length=255, blank=True, null=True)
    suffix = models.CharField(max_length=255, blank=True, null=True)
    addr1 = models.CharField(max_length=255, blank=True, null=True)
    addr2 = models.CharField(max_length=255, blank=True, null=True)
    city = models.CharField(max_length=255, blank=True, null=True)
    zip = models.CharField(max_length=255, blank=True, null=True)
    gender = models.CharField(max_length=255, blank=True, null=True)
    race = models.CharField(max_length=255, blank=True, null=True)
    birthdate = models.CharField(max_length=255, blank=True, null=True)
    party = models.CharField(max_length=255, blank=True, null=True)
    areacode = models.CharField(max_length=255, blank=True, null=True)
    phone = models.CharField(max_length=255, blank=True, null=True)
    email = models.CharField(max_length=255, blank=True, null=True)
    voter_id = models.CharField(max_length=255, blank=True, null=True)
    county_slug = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        ordering = ['lname', 'fname']

    def __str__(self):
        bits = [bit for bit in [self.fname, self.mname, self.lname] if bit]
        name = ' '.join(bits)
        if self.suffix and SUFFIXES.get(self.suffix.upper()):
            name += ', {}'.format(SUFFIXES.get(self.suffix.upper()))
        return name

    @property
    def county(self):
        return COUNTY_MAP.get(self.county_slug)
