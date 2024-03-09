import datetime
from django.dispatch import receiver
from django.db.models.signals import post_save
from .models import Filmwork


@receiver(post_save, sender=Filmwork)
def attention(sender, instance, created, **kwargs):
    if created and instance.creation_date == datetime.date.today():
        print(f"Сегодня премьера {instance.title}! 🥳")
