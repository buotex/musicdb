from django.shortcuts import get_object_or_404, render
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.views.generic import ListView

from .models import Composition

class IndexView(ListView):
    model = Composition
    template_name = 'data/index.html'



def index(request):
    return HttpResponse("Hello, world. You're at the polls index.")

def enter_data(request):
    return HttpResponse("Hello, world. You're at the polls entry.")

def compositions(request):
    objs = Composition.objects.all()
    output = '\n '.join([obj.title for obj in objs])
    return HttpResponse(output)


# Create your views here.
