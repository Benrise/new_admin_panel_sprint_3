import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class Role(models.TextChoices):
    DIRECTOR = 'director', _('Director')
    WRITER = 'writer', _('Writer')
    ACTOR = 'actor', _('Actor')


class Type(models.TextChoices):
    MOVIE = "movie", _('movie')
    TV_SHOW = "tv_show", _('tv_show')


class TimeStampedMixin(models.Model):
    created_at = models.DateTimeField(_('created_at'), auto_now_add=True)
    updated_at = models.DateTimeField(_('updated_at'), auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('fullname'), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('persons')

    def __str__(self):
        return self.full_name


class Filmwork(UUIDMixin, TimeStampedMixin):
    title = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)
    creation_date = models.DateField(_('creation_date'), blank=True, null=True)
    file_path = models.TextField(_('file_path'), blank=True, null=True)
    rating = models.FloatField(_('rating'),
                               default=0.0,
                               validators=[MinValueValidator(0.0),
                                           MaxValueValidator(10)],
                               blank=True,
                               null=True)
    type = models.CharField(_('type'), max_length=128, choices=Type.choices)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork', null=True)
    persons = models.ManyToManyField(Person, through='PersonFilmwork', null=True)

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('film_work')
        verbose_name_plural = _('film_works')

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE,
                                  verbose_name=_('film_work'))
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE,
                              verbose_name=_('person'))
    created_at = models.DateTimeField(_('created_at'), auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('genre_film_work')
        verbose_name_plural = _('genre_film_works')
        unique_together = ['genre', 'film_work']


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE,
                                  verbose_name=_('film_work'))
    person = models.ForeignKey('Person', on_delete=models.CASCADE,
                               verbose_name=_('person'))
    role = models.CharField(_('role'), max_length=20, choices=Role.choices)
    created_at = models.DateTimeField(_('created_at'), auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('person_film_work')
        verbose_name_plural = _('person_film_works')
        unique_together = ['film_work', 'person', 'role']
