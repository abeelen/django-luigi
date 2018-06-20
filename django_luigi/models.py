from django.db import models

# Create your models here.


class TableUpdates(models.Model):
    update_id = models.CharField(max_length=128, null=False, primary_key=True)
    target_model = models.CharField(max_length=128)
    inserted = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'table_updates'
