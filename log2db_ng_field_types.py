#!/usr/local/bin/python -Wignore::DeprecationWarning
# -*- coding: utf-8 -*-

import re
import urllib
import urlparse
import math

import geoip2.database
import geoip2.errors

geoip_city = geoip2.database.Reader('/usr/local/share/GeoIP/GeoIP2-City.mmdb') 

class LogField(object):
    def __init__(self, value, *args, **kwargs):
        self.value = value

        super(LogField, self).__init__(*args, **kwargs)

    def clean(self):
        return self.value

class IPv4Field(LogField):
    def clean(self):
        assert re.match('([0-2]?([0-5]|(?<![2-9])[0-9])?([0-5]|(?<!25)[0-9])\.){4}', self.value + '.')

        return self.value
    
class GeoIP2CityDBField(IPv4Field):
    db = None
    ip = None

    def __init__(self, value, *args, **kwargs):
        super(GeoIP2CityDBField, self).__init__(value, *args, **kwargs)

    def clean(self):
        self.value = super(GeoIP2CityDBField, self).clean()

        if self.ip != self.value:
            try:
                GeoIP2CityDBField.db = geoip_city.city(self.value)
            except geoip2.errors.AddressNotFoundError:
                GeoIP2CityDBField.db = None

            GeoIP2CityDBField.ip = self.value

class GeoIP2CityDBCityField(GeoIP2CityDBField):
    def clean(self):
        self.value = super(GeoIP2CityDBCityField, self).clean()

        try:
            return  { \
                        'id':       self.db.city.geoname_id, \
                        'title':    self.db.city.names['en'] if 'en' in self.db.city.names else None, \
                        'title_ru': self.db.city.names['ru'] if 'ru' in self.db.city.names else None, \
                    }
        except:
            return ''

class GeoIP2CityDBRegionField(GeoIP2CityDBField):
    def clean(self):
        self.value = super(GeoIP2CityDBRegionField, self).clean()

        try:
            return  { \
                        'id':       self.db.subdivisions[0].geoname_id, \
                        'iso_code': self.db.subdivisions[0].iso_code, \
                        'title':    self.db.subdivisions[0].names['en'] if 'en' in self.db.subdivisions[0].names else None, \
                        'title_ru': self.db.subdivisions[0].names['ru'] if 'ru' in self.db.subdivisions[0].names else None, \
                    }
        except:
            return ''

class GeoIP2CityDBCountryField(GeoIP2CityDBField):
    def clean(self):
        self.value = super(GeoIP2CityDBCountryField, self).clean()

        try:
            return  { \
                        'id':       self.db.country.geoname_id, \
                        'iso_code': self.db.country.iso_code, \
                        'title':    self.db.country.names['en'] if 'en' in self.db.country.names else None, \
                        'title_ru': self.db.country.names['ru'] if 'ru' in self.db.country.names else None, \
                    }
        except:
            return ''

class IntField(LogField):
    def clean(self):
        if isinstance(self.value, basestring):
            if len(self.value) == 0:
                return None

        return int(self.value)

class FloatField(LogField):
    def clean(self):
        if isinstance(self.value, basestring):
            if len(self.value) == 0:
                return None

        res = float(self.value)
        if math.isnan(res):
            return None
        if math.isinf(res):
            return None

        return res
 
class NullableField(LogField):
    def clean(self):
        if isinstance(self.value, basestring):
            if self.value.lower() == 'undefined':
                return None

        return self.value

class EscapedField(LogField):
    def clean(self):
        return self.value.encode('string-escape') 

class MultiLineField(LogField):
    def clean(self):
        return ''.join(self.value.splitlines())

class URLDecodedField(LogField):
    def clean(self):
        return urllib.unquote(self.value).decode('utf8', 'replace').encode('utf8')

class RecursiveField(LogField):
    def clean(self):
        action_value = self.field_class(self.value).clean()

        if self.value == action_value:
            return self.value

        self.value = action_value
        
        return self.clean() 

def RecursiveFieldType(field_class):
    res = RecursiveField 
    res.field_class = field_class 

    return res

class RegexpField(LogField):
    def clean(self):
        assert re.match(self.regexp, self.value)

        return self.value

class LowerField(LogField):
    def clean(self):
        return self.value.lower()


def LimitedLengthFieldType(length_limit):
    res = LimitedLengthField
    res.length_limit = length_limit

    return res

class LimitedLengthField(LogField):
    def clean(self):
        return self.value[:self.length_limit]

class MultiTraitField(LogField):
    def clean(self):
        for base_class in type(self).__bases__:
            if base_class != MultiTraitField:
                self.value = base_class(self.value).clean()
                if self.value is None:
                    return self.value

        return self.value

class GUIDField(RegexpField):
    regexp = '(?i)[0-9a-f]{8}-?([0-9a-f]{4}-?){3}[0-9a-f]{12}'

class LogGUIDField(MultiTraitField, NullableField, GUIDField, LowerField):
    pass

class LogFloatField(MultiTraitField, NullableField, FloatField):
    pass

class LogCharField(MultiTraitField, NullableField, RecursiveFieldType(URLDecodedField)):
    pass

class TimestampField(MultiTraitField, FloatField, IntField):
    pass

class RefererField(MultiTraitField, RecursiveFieldType(URLDecodedField), EscapedField, LimitedLengthFieldType(1024)):
    def clean(self):
        super(RefererField, self).clean()

        o = urlparse.urlsplit(self.value)

        if len(o.scheme) == 0:
            o = urlparse.urlsplit('undef://%s' % self.value)

        hostname = o.hostname or ''

        return  { \
                    'scheme':   o.scheme, \
                    'netloc':   hostname, \
                    'colten':   '.'.join(reversed(hostname.split('.'))), \
                    'path':     o.path, \
                    'query':    urlparse.parse_qs(o.query), \
                    'site':     '.'.join(hostname.split('.')[-2:]), \
                }

def ErrorFieldType(error_type):
    res = ErrorField
    res.error_type = error_type

    return res

class ErrorField(MultiTraitField, RecursiveFieldType(URLDecodedField), EscapedField):
    def clean(self):
        super(ErrorField, self).clean()

        res = {}
        res['name'] = self.error_type

        field_parts = re.split('[,]', self.value)

        try:
            res['code'] = int(field_parts[0])
            res['msg'] = field_parts[1]
        except:
            res['msg'] = self.value

        return res

class UserAgentField(MultiTraitField, URLDecodedField, EscapedField, LimitedLengthFieldType(1024)):
    pass
