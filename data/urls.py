from django.conf.urls import url

from data.views import IndexView

app_name='data'
urlpatterns = [
    url(r'^$', IndexView.as_view(), name='index'),

    #url(r'^enter_data$', views.enter_data, name='enter_data'),
    #url(r'^compositions$', views.compositions, name='compositions')

]