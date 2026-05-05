"""
demographics.py — classification prénom / genre / culture / oum-abou

Sources :
- prénoms maghrébins / arabes courants (curated)
- prénoms français / européens courants (curated)
- heuristiques terminaisons pour le fallback
- regex oum/abou variantes (umm, ou, oummi, abu, abou, abbou)

Toutes les comparaisons se font sur du texte normalisé (lower, sans accents,
sans chiffres, sans ponctuation).
"""

import re
import unicodedata


# =====================================================================
# Dictionnaires (prénoms les plus courants — couverture pragmatique)
# =====================================================================

# Prénoms féminins arabo-maghrébins (~200)
FEMALE_AR = {
    "aicha","aisha","aysha","amina","amira","amine","asma","asmaa","assia","aya","ayat",
    "bahia","basma","bouchra","chaima","chaimaa","chahinaz","chahrazade","cherifa",
    "dalila","dounia","douaa","douae","dorra","djamila","djemila","djoumana",
    "fadila","fadwa","fadhila","faiza","faiqa","farida","faten","fatiha","fatima","fatma",
    "fatimazahra","fatimazohra","fawzia","feriel","ferielle","ferdaous","firdaws","ferdaws",
    "ghada","ghizlaine","ghizlane",
    "habiba","hadda","hafsa","hajar","hajer","hakima","halima","hamida","hanaa","hana","hanae",
    "hanan","hanane","hayat","hela","hiba","hind","hinda","houda","houria","hosna",
    "ikram","ikrame","imane","imen","ines","inas","inass","ismahane","iman",
    "jamila","jasmine","jihane","joumana","joumayna",
    "kaltoum","kaouthar","karima","keltoum","kenza","khadija","khaira","khalida","khawla","kheira","khedidja",
    "lalla","lamia","lamiae","lamis","latifa","leila","leyla","lila","lina","linda","lobna","loubna","loujain","lubna",
    "maha","malak","malika","manel","mariem","mariam","maryam","maysoun","melissa","meriem","meryem","milouda","mounia",
    "mouna","myriam","najat","najia","najla","najwa","nora","norah","noura","noor","nour","naima","nadia","nadira","narimane",
    "nawal","neila","nesrine","nezha","nihale","nour","nouria",
    "ouafa","ouafae","oumayma","oumelghait",
    "rabia","rachida","radia","radya","rahma","rajaa","rajae","rania","ranya","rasha","rawan","rawya","razika",
    "rabab","rabia","rim","rime","ryma","rita",
    "saadia","sabah","sabrina","safa","safae","safia","sakina","salama","salima","salma","salwa","samia","samira","sana","sanae","sara","saraa","sarah","sawsane","sawsen","selma","selima","sendes","siham","sihem","siwar","sokaina","souad","souhaila","soumeya","soumia",
    "thouraya","tassadit","tessadite",
    "wafaa","wafa","wahiba","warda","wassila","widad","wissam","wijdane",
    "yamina","yasmina","yasmine","yousra",
    "zahia","zahira","zahra","zahraa","zaina","zayna","zineb","zineba","zora","zohra","zoulikha"
}

# Prénoms masculins arabo-maghrébins (~200)
MALE_AR = {
    "abdelaziz","abdelhakim","abdelhalim","abdelilah","abdelkader","abdelkarim","abdellah","abdellatif","abdelmadjid","abdelmajid","abdelmalek","abdelmoumene","abdelnour","abderrahim","abderrahmane","abderrazak","abdou","abdoul","abdul","abdullah","abdulrahman","abou","aboubacar","aboubakr","achraf","adam","adel","adil","ahmad","ahmed","aimen","akram","ali","alaa","aladdin","amin","amine","amir","amjad","ammar","anas","anouar","arif","ashraf","assaad","ayoub","aziz","azzedine",
    "bachir","badr","badreddine","bahaa","bilal","boualem","brahim","badis","belkacem","benamar",
    "chakib","chakir","chouaib","cherif","chiheb",
    "djamel","djamil","djebril","driss",
    "elhadi","elhadj","elias","elyes","essam","ezzeddine",
    "fadi","fahd","faical","faouzi","faisal","fares","farid","faycal","ferhat","ferid","firas","fouad","fouzi",
    "ghaleb","ghassan","ghani",
    "habib","hachem","hafid","haitham","hakim","halim","hamid","hamza","haroun","harun","hassan","hatim","hicham","hichem","hilal","hisham","houssam","houssem","houcine","hussain","hussein","hocine",
    "ibrahim","idris","idriss","ilyas","imad","imran","ismail","ismael","issa","issam","iyad",
    "jad","jalal","jalil","jamal","jamel","jaouad","javed","jihad","jibril","jonas","jaweed",
    "kacem","kader","kais","kamel","kamil","karim","kassim","khaled","khalid","khalifa","khalil","khaireddine",
    "lakhdar","larbi","lokmane","lotfi","loukmane","luqman",
    "maamar","mahdi","mahmoud","majd","malik","mansour","mehdi","mehdi","mehran","menad","messaoud","miloud","mohamed","mohammad","mohammed","mokhtar","mostefa","mouhsine","mounir","mourad","moussa","mouhssin","mouhssine","mouhamed",
    "naceur","nacer","nadhir","nadhem","nadim","nadir","naim","nasser","nasreddine","nawfel","nazim","nizar","noah","noureddine","nouri",
    "okba","omar","omer","osama","othman","oussama",
    "rabah","rabie","rachid","rafik","rafiq","rahim","raouf","rashad","rashid","rayan","rayane","redouane","reda","ridha","riad","riyad","rida","ryad","rizk",
    "saad","sadek","sadik","said","saif","salah","saleh","salem","salim","salman","sami","samir","sayf","sefiane","sehbi","selim","seif","seifeddine","sherif","sid","sidahmed","sidi","sliman","sofiane","souhail","sufyan","suleiman","sultan",
    "tahar","taher","tarek","tarik","tariq","tayeb","toufik","toufiq",
    "walid","wassim","waleed","wessim",
    "yacine","yahya","yacoub","yamine","yanis","yasin","yasine","yassin","yassine","yazid","younes","younis","younous","youssef","yousef","yusuf","yacin",
    "zaki","zakaria","zakariae","zakariya","zakaria","ziad","zied","zineddine","zoheir","zohir","zouhir"
}

# Prénoms féminins européens / FR (~150)
FEMALE_EU = {
    "agathe","agnes","alexandra","alice","alicia","aline","amandine","amelie","anais","anaelle","andrea","angele","angelique","anne","annie","ariane","audrey","aurelie","aurore",
    "barbara","beatrice","benedicte","bernadette","brigitte","bertille",
    "camille","capucine","caroline","cassandra","catherine","cecile","celine","celia","celestine","chantal","charlene","charlotte","chloe","christelle","christine","christiane","cindy","claire","clarisse","claudia","claudine","clemence","clementine","clothilde","colette","constance","coralie","corinne","cyrielle",
    "daphne","delphine","denise","diane","dominique","dorothee",
    "edith","eleonore","elisa","elisabeth","elise","elodie","emma","emeline","emilie","estelle","esther","eugenie","eva","eve","evelyne",
    "fanny","faustine","fiona","flavie","florence","florine","francoise","frederique",
    "gabrielle","gaelle","gaetane","genevieve","georgette","ghislaine","gisele",
    "helene","henriette",
    "ines","ingrid","irene","isabelle",
    "jacqueline","jade","jeanne","jeannine","jennifer","jessica","jocelyne","josephine","josette","julia","julie","juliette","justine",
    "karine","kathleen",
    "laetitia","laura","laure","laurence","lea","leila","leonie","liliane","lola","louise","lucie","ludivine",
    "madeleine","madelaine","maelle","maeva","manon","marcelle","margaux","marguerite","marianne","marie","marion","marjorie","marlene","martine","mathilde","mauricette","melanie","melissa","mireille","monique","morgane","muriel","myriam",
    "nadege","nadine","nathalie","nicole","noelle","noemie",
    "ophelie","odile","olivia",
    "patricia","paula","paulette","pauline","peggy","perrine","prisca",
    "rachelle","raphaelle","raymonde","regine","renee","rolande","romane","rosalie","rose","roselyne",
    "sabine","sandrine","sarah","sasha","severine","sidonie","simone","solange","solene","sonia","sophie","stephanie","susan","suzanne","sylvie",
    "therese","tiphaine",
    "valentine","valerie","vanessa","veronique","vianne","victoire","violette","virginie",
    "yolande","yvette","yvonne",
    "zoe","zelie"
}

# Prénoms masculins européens / FR (~150)
MALE_EU = {
    "adrien","aime","alain","alban","albert","alexandre","alexis","alfred","amaury","ambroise","anatole","andre","anthony","antoine","antonin","armand","armel","arnaud","arthur","auguste","augustin","aurelien","axel","aymeric",
    "baptiste","bastien","benjamin","benoit","bernard","bertrand","bruno",
    "cedric","cesar","charles","christian","christophe","claude","clement","colin","corentin","cyril","cyrille",
    "damien","daniel","david","denis","didier","dimitri","dominique",
    "edouard","edmond","edgar","emile","emilien","emmanuel","enzo","eric","etienne","eugene","evan","ezechiel",
    "fabien","fabrice","felix","ferdinand","florent","florian","francis","francois","frederic",
    "gabriel","gaetan","gael","gaspard","gaston","gauthier","geoffrey","georges","gerald","gerard","germain","gilbert","gilles","gregoire","gregory","guillaume","guy",
    "hadrien","henri","herve","hippolyte","hubert","hugo","hugues",
    "ivan",
    "jacky","jacques","janvier","jason","jean","jeremie","jeremy","jerome","jimmy","joachim","joel","joffrey","jonathan","joris","joseph","josselin","jules","julien","justin","jeanmarc","jeanphilippe","jeanmichel","jeanluc","jeanjacques","jeanmarie",
    "kenny","kevin",
    "laurent","leo","leon","leonard","leopold","lilian","lionel","loic","loris","louis","loup","luc","lucas","lucien","ludovic",
    "marc","marcel","marius","martin","mathieu","mathis","mathys","matthias","matthieu","mattheo","mattias","maurice","max","maxence","maxime","mickael","michael","michel","milan","mohamed","morgan",
    "nathan","nicolas","noel","norbert",
    "octave","odilon","olivier","oscar","oswald","owen",
    "pascal","patrice","patrick","paul","pierre","pierrick","pol",
    "quentin",
    "rafael","raphael","raymond","regis","remi","remy","renaud","rene","richard","robert","robin","rodolphe","rodrigue","roger","roland","rolland","romain","romuald","ronan","roland",
    "samuel","sebastien","serge","sergio","silvain","simon","stanislas","stephane","sylvain","sylvestre",
    "tanguy","theo","theodore","theophile","thibaud","thibault","thibaut","thierry","thomas","timothee","tom","tony","tristan",
    "ulysse",
    "valentin","valery","vianney","victor","vincent","vivien",
    "william","willy",
    "xavier",
    "yann","yannick","yannis","yoan","yoann","yohann","yvan","yves",
    "zacharie"
}

# Prénoms mixtes (passe-partout : maghreb + europe ou origine ambiguë)
MIXED = {
    "adam","adele","aida","alba","alia","aliya","alma","alya","aliyah","aria","ariane","ayan","aylin","ayman",
    "celia","celya","clara","cyrine","cyriane",
    "dalia","dunya","dania","diana","daria","darya","dilan",
    "eden","eliyah","elinor","ellie","elsa","emir","emina",
    "ines","iman","inaya","ilan","isra","israel","ismael","ismael",
    "kayla","kayla","kyria",
    "laura","lara","layla","leyna","leah","lena","lia","liana","lila","lina","liya","lola",
    "maya","mayssa","melia","melyssa","mia","mila","milena","mira","mona","myla",
    "naomi","nour","noor","noa","noah","norah","nina","nola",
    "rania","rayan","rayane","ria","rita","rosa","ryma","rim",
    "safia","salma","samia","samir","sami","sana","sara","sarah","sasha","selma","sofia","soraya","syrine",
    "talia","tania","thara","tina",
    "yara","yasmine","yara","yasmin",
    "zara","zaya","zelia","ziva","zoe","zoé","zora","zoya"
}

# =====================================================================
# Email domain classification
# =====================================================================

PERSONAL_DOMAINS = {
    # Google
    "gmail.com", "googlemail.com",
    # Microsoft
    "hotmail.com", "hotmail.fr", "hotmail.co.uk", "hotmail.es", "hotmail.it",
    "hotmail.de", "hotmail.be", "hotmail.ca",
    "outlook.com", "outlook.fr", "outlook.es", "outlook.it", "outlook.de", "outlook.be",
    "live.com", "live.fr", "live.co.uk", "live.be", "live.ca",
    "msn.com",
    # Apple
    "icloud.com", "me.com", "mac.com",
    # Yahoo
    "yahoo.com", "yahoo.fr", "yahoo.co.uk", "yahoo.es", "yahoo.it", "yahoo.de",
    "yahoo.co.in", "yahoo.be", "yahoo.ca", "yahoo.com.ar",
    # French ISPs
    "sfr.fr", "sfr.com", "orange.fr", "orange.com",
    "free.fr", "laposte.net", "wanadoo.fr", "neuf.fr", "bbox.fr",
    "aliceadsl.fr", "numericable.fr", "noos.fr", "cegetel.net",
    "caramail.com", "voila.fr", "club-internet.fr",
    # Privacy / encrypted
    "protonmail.com", "proton.me", "pm.me", "tutanota.com", "tutanota.de", "tuta.io",
    # Others
    "yandex.ru", "yandex.com", "yandex.fr",
    "mail.com", "email.com", "gmx.fr", "gmx.com", "gmx.de", "gmx.net",
    "aol.com", "mailinator.com", "guerrillamail.com",
    # Maghreb providers
    "menara.ma", "iam.ma",
}

APPLE_DOMAINS = {"icloud.com", "me.com", "mac.com"}

# Unambiguous country-code TLDs
TLD_COUNTRY = {
    "fr": "FR", "ma": "MA", "dz": "DZ", "tn": "TN",
    "be": "BE", "ca": "CA", "de": "DE", "es": "ES",
    "it": "IT", "nl": "NL", "ch": "CH", "lu": "LU",
    "pt": "PT", "se": "SE", "no": "NO", "dk": "DK",
    "re": "RE", "mq": "MQ", "gp": "GP", "nc": "NC",
    "ae": "AE", "sa": "SA", "eg": "EG", "ly": "LY",
    "mr": "MR", "sn": "SN", "ci": "CI", "cm": "CM",
    "gn": "GN", "bf": "BF", "ne": "NE", "td": "TD",
    "uk": "GB", "pl": "PL", "ro": "RO", "gr": "GR",
}


def classify_email_domain(email: str) -> dict:
    """
    Returns:
      is_pro_email      — not a known personal/free provider (bool or None)
      email_tld_country — 2-letter country from TLD, None if .com/.net/etc.
      is_education      — academic / public-education domain (bool)
      is_apple          — icloud / me / mac domain (bool)
    """
    if not email or "@" not in email:
        return {"is_pro_email": None, "email_tld_country": None,
                "is_education": False, "is_apple": False}

    domain = email.split("@", 1)[1].strip().lower()

    is_apple = domain in APPLE_DOMAINS

    # Education detection (before personal check — edu is institutional)
    is_education = bool(
        domain == "education.gouv.fr"
        or domain.endswith(".edu")
        or domain.endswith(".ac.uk")
        or re.match(r"^ac-[a-z]", domain)          # ac-paris.fr, ac-lyon.fr
        or re.match(r"^univ-[a-z]", domain)         # univ-paris.fr
        or re.match(r"^u-[a-z]", domain)            # u-bordeaux.fr
        or re.match(r"^ens[a-z]?\.fr$", domain)     # ens.fr, ensl.fr
        or re.search(r"(universite|university|universi)", domain)
        or re.search(r"(chu-|\.chu\.)", domain)      # teaching hospitals
        or re.search(r"(lycee|college\.edu)", domain)
    )

    is_personal = (domain in PERSONAL_DOMAINS) and not is_education
    is_pro_email = not is_personal and not is_apple

    # Country from TLD
    parts = domain.split(".")
    tld = parts[-1] if parts else ""
    email_tld_country = TLD_COUNTRY.get(tld)
    # Handle second-level like .co.uk → tld already = "uk"

    return {
        "is_pro_email": is_pro_email,
        "email_tld_country": email_tld_country,
        "is_education": is_education,
        "is_apple": is_apple,
    }


# Termes religieux / culturels qui apparaissent dans les emails (signal "convertie" possible)
RELIGIOUS_HINTS = {
    "ukhty","ukhti","akhi","deen","dine","hijra","sunni","muslima","muslim","oumma",
    "salaf","salafi","sahaba","amira","fillah","jannah","ahleslmi"
}


# =====================================================================
# Helpers
# =====================================================================

def _normalize(s: str) -> str:
    """Lower-case, strip accents, keep only [a-z]."""
    if not s:
        return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z]+", "", s)
    return s


_PREFIXES = {"oum", "umm", "ummu", "ummi", "ouma", "abou", "abu", "abbou"}
_RE_PREFIX_GLUED = re.compile(
    r"^(?:oum+m?[iey]?|umm+u?[iy]?|abou+|abu+|abbou+)([a-z]{2,})$"
)


def _strip_prefix(token: str) -> str:
    """Si le token est 'oumzineb' ou 'abouyahya', retourne 'zineb' / 'yahya'."""
    m = _RE_PREFIX_GLUED.match(token)
    if m:
        return m.group(1)
    return token


def extract_firstname(shop_firstname: str, ml_name: str, email: str) -> str:
    """
    Extrait un prénom hiérarchiquement :
    1. firstname côté Presta (skipper si c'est juste 'oum'/'abou' isolé)
    2. premier mot non-préfixe de ml_name
    3. premier segment alpha de l'email (avec strip oum/abou collé)
    Returns lower-case, normalized.
    """
    # 1. shop firstname
    if shop_firstname and shop_firstname.strip():
        for part in shop_firstname.strip().split():
            norm = _normalize(part)
            if norm and norm not in _PREFIXES and len(norm) >= 2:
                return _strip_prefix(norm)

    # 2. ml name : 1er mot non préfixe
    if ml_name and ml_name.strip():
        for w in ml_name.strip().split():
            norm = _normalize(w)
            if norm and norm not in _PREFIXES and len(norm) >= 2:
                return _strip_prefix(norm)

    # 3. email — premier segment alpha non préfixe
    if not email or "@" not in email:
        return ""
    local = email.split("@", 1)[0]
    parts = re.split(r"[._\-+]+", local)
    for p in parts:
        norm = _normalize(p)
        if norm and not norm.isdigit() and norm not in _PREFIXES and len(norm) >= 2:
            return _strip_prefix(norm)
    return ""


# =====================================================================
# Classification gender
# =====================================================================

# Terminaisons typiques féminines
F_ENDINGS = ("a", "ah", "ia", "ya", "iya", "ina", "ette", "elle", "ie")

def classify_gender(firstname_norm: str) -> str:
    """Returns 'M', 'F' or 'U'."""
    if not firstname_norm:
        return "U"
    if firstname_norm in FEMALE_AR or firstname_norm in FEMALE_EU:
        return "F"
    if firstname_norm in MALE_AR or firstname_norm in MALE_EU:
        return "M"
    if firstname_norm in MIXED:
        # Heuristique terminaison
        for end in F_ENDINGS:
            if firstname_norm.endswith(end):
                return "F"
        return "U"
    # Fallback heuristique pure
    for end in F_ENDINGS:
        if firstname_norm.endswith(end) and len(firstname_norm) >= 4:
            return "F"
    # Masculin : se termine par consonne dure et au moins 4 lettres
    if len(firstname_norm) >= 4 and firstname_norm[-1] in "bcdfgklmnprstvz":
        return "M"
    return "U"


# =====================================================================
# Classification culture
# =====================================================================

def classify_culture(firstname_norm: str) -> str:
    """Returns 'maghreb', 'europe', 'mixed', 'unknown'."""
    if not firstname_norm:
        return "unknown"
    in_ar = firstname_norm in FEMALE_AR or firstname_norm in MALE_AR
    in_eu = firstname_norm in FEMALE_EU or firstname_norm in MALE_EU
    in_mx = firstname_norm in MIXED
    if in_ar and not in_eu:
        return "maghreb"
    if in_eu and not in_ar:
        return "europe"
    if in_mx or (in_ar and in_eu):
        return "mixed"
    return "unknown"


# =====================================================================
# Detection oum / abou (préfixes email courants)
# =====================================================================

# Regex curées pour oum / abou — strictes pour éviter faux positifs (martin, marie...)
# OUM matche : oum, oumm, oummi, ouma, ummu, umm, ummi suivi par lettres ou séparateur
RE_OUM  = re.compile(r"(?:^|[\._\-])(?:oum+m?[iey]?|umm+u?[iy]?)(?=[a-z]{2,}|[\._\-]|$)", re.IGNORECASE)
# ABOU matche : abou, abu, abbou suivi par lettres ou séparateur
RE_ABOU = re.compile(r"(?:^|[\._\-])(?:abou+|abu+|abbou+)(?=[a-z]{2,}|[\._\-]|$)", re.IGNORECASE)

def detect_oum_abou(email: str, firstname_raw: str = "", ml_name_raw: str = ""):
    """
    Returns (is_oum, is_abou).
    Recherche dans l'email (préfixe local) ET dans firstname/ml_name s'ils contiennent
    'oum xxx' ou 'abou xxx'.
    """
    is_oum = is_abou = False

    if email and "@" in email:
        local = email.split("@", 1)[0].lower()
        if RE_OUM.search(local):
            is_oum = True
        if RE_ABOU.search(local):
            is_abou = True

    # Aussi dans le name si présent : "oum xxx", "abou xxx" en début
    for src in (firstname_raw or "", ml_name_raw or ""):
        s = src.lower().strip()
        if not s:
            continue
        if re.match(r"^(?:oum+m?[iey]?|umm+u?[iy]?)\s", s):
            is_oum = True
        if re.match(r"^(?:abou+|abu+|abbou+)\s", s):
            is_abou = True

    return is_oum, is_abou


# =====================================================================
# Quick tests
# =====================================================================

if __name__ == "__main__":
    tests = [
        # (shop_first, ml_name, email)         → expected_first / gender / culture
        ("Aïcha",  "",                "aicha.benzema@gmail.com"),
        ("Hakim",  "",                "hakim.fellah@yahoo.fr"),
        ("",       "Sarah",           "sarah_2010@hotmail.fr"),
        ("",       "",                "marie.dupont@orange.fr"),
        ("",       "",                "oumzineb@gmail.com"),
        ("",       "Abou Yahya",      "abouyahya93@hotmail.fr"),
        ("Pierre", "",                "pierre.martin@free.fr"),
        ("",       "ummu mariam",     "umm.mariam@gmail.com"),
        ("",       "",                "kelyana974@icloud.com"),
        ("",       "leno",            "leno@example.com"),
    ]
    for shop, ml, email in tests:
        first = extract_firstname(shop, ml, email)
        g = classify_gender(first)
        c = classify_culture(first)
        oum, abou = detect_oum_abou(email, shop, ml)
        print(f"  shop={shop:8s} ml={ml:15s} email={email:35s} → first={first:15s} g={g} c={c:8s} oum={oum} abou={abou}")
