#!/bin/bash


python manage.py check --deploy

python manage.py migrate --no-input

python manage.py createsuperuser --no-input \
    --username $DJANGO_SUPERUSER_USERNAME \
    --email $DJANGO_SUPERUSER_EMAIL

python manage.py collectstatic --no-input

uwsgi --strict --ini uwsgi.ini
