from django.db import connection
from django.core.exceptions import ImproperlyConfigured
from django.test.client import FakePayload


class MultiDBMiddleware(object):
    """Provide database affinity persistence between requests.

    This depends on SessionMiddleware being loaded. It might not work
    well if the session backend uses a replicated database.

    """
    def process_request(self, request):
        """Load affinity information for a session"""
        if not hasattr(request, 'session'):
            raise ImproperlyConfigured(
                "MultiDBMiddleware requires SessionMiddleware loaded first.")

        if hasattr(connection, 'mapper'):
            if isinstance(request.META['wsgi.input'], FakePayload):
                # Don't clear when using the test client
                return None
            connection.mapper._affinity = request.session.get(
                '_mdb_affinity', {})
            connection.mapper._affinity_modified = False

    def process_response(self, request, response):
        """Save affinity information for a session"""
        if hasattr(connection, 'mapper') and hasattr(request, 'session'):
            if connection.mapper._affinity_modified:
                connection.mapper._clean_affinity()
                request.session['_mdb_affinity'] = connection.mapper._affinity
        return response
