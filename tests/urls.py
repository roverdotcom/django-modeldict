from django.urls import re_path


def dummy_view(request):
    from django.http import HttpResponse
    return HttpResponse()


urlpatterns = [
    re_path(r'^$', dummy_view, name='modeldict-home'),
]
