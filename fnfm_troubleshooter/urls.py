from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
import os 

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('troubleshooter_app.urls')), # Include your app's URLs
]

# Serve static files during development
if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
    # This is crucial for pyvis graph HTML files to be accessible
    urlpatterns += static('/static/graphs/', document_root=os.path.join(settings.BASE_DIR, 'static', 'graphs'))