
class MissingTagException (Exception):
    pass

class OverlapException (Exception):
    """ Data insertion would overlap existing data """
    pass

class TimeoutException (Exception):
    pass

class NoConfigKeyException (Exception):
    """ A process does not define a config lookup """
    pass

class NoConfigException (Exception):
    """ A process instance does not have a matching config """

    def __init__ (self, target_tags, match_tags):
        super().__init__("<tags='%s' match='%s'>" % (target_tags, match_tags))

