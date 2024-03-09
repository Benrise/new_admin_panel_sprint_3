from django.http import JsonResponse
from django.views.generic.list import BaseListView
from django.views.generic.detail import BaseDetailView
from django.db.models import Q
from django.contrib.postgres.aggregates import ArrayAgg

from movies.models import Filmwork, Role


class FilmworkApiMixin:
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self):
        qs = Filmwork.objects.prefetch_related('genres', 'persons').values().annotate(
            genres=ArrayAgg(
                'genres__name',
                distinct=True
            ),
            actors=ArrayAgg(
                'persons__full_name',
                distinct=True,
                filter=Q(personfilmwork__role=Role.DIRECTOR),
            ),
            directors=ArrayAgg(
                'persons__full_name',
                distinct=True,
                filter=Q(personfilmwork__role=Role.DIRECTOR),
            ),
            writers=ArrayAgg(
                'persons__full_name',
                distinct=True,
                filter=Q(personfilmwork__role=Role.WRITER),
            ),
        )
        return qs

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(FilmworkApiMixin, BaseListView):
    paginate_by = 50

    def get_context_data(self, *, object_list=None, **kwargs):
        qs = self.get_queryset()
        paginator, page, queryset, is_paginated = self.paginate_queryset(
            qs,
            self.paginate_by
        )
        context = {
            'count': paginator.count,
            'total_pages': page.paginator.num_pages,
            'prev': page.previous_page_number() if page.number > 1 else None,
            'next': page.next_page_number() if page.has_next() else None,
            'results': list(queryset),
        }
        return context


class MoviesDetailApi(FilmworkApiMixin, BaseDetailView):    
    def get_context_data(self, **kwargs):
        return self.get_object()
