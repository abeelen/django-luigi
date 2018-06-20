# -*- coding: utf-8 -*-
#
# Copyright (c) 2018 Alexandre Beelen
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
"""
Support for Django ORM. Provide DjangoTarget for storing objects in django databases.
The user is responsible for installing and configuring a django instance, including
the django_luigi app. (mostly based on luigi.contib.sqla)

INSTALLED_APP = [...
     'django_luigi',
     ]

Assuming a django model Test in the test app with two CharField, item1, and item2, properly migrated

.. code-block:: python
    from django.db import models

    class Test(models.Model):
        item1 = models.CharField(max_length=120)
        item2 = models.CharField(max_length=120)

Here is a minimal example of a job to copy data to database using Django :

.. code-block:: python

    import luigi
    from django_luigi.django import ToORM

    class TestTask(ToORM):

        django_root = 'path_to_your_django_root' # Where manage.py is
        settings_path = 'my_site.settings'

        model = 'Test'
        app = 'test'

        def rows(self):
            for row in [("item1", "property1"), ("item2", "property2")]:
                yield row

    if __name__ == '__main__':
        task = TestTask()
        luigi.build([task], local_scheduler=True)


In the above examples, the data that needs to be copied was directly provided by
overriding the rows method. Alternately, if the data comes from another task, the
modified example would look as shown below:

.. code-block:: python

    import luigi
    from django_luigi.django import ToORM
    from luigi.mock import MockTarget

    class BaseTask(luigi.Task):
        def output(self):
            return MockTarget("BaseTask")

        def run(self):
            out = self.output().open("w")
            TASK_LIST = ["item%d\\tproperty%d\\n" % (i, i) for i in range(1,3)]
            for task in TASK_LIST:
                out.write(task)
            out.close()

    class TestTask(ToORM):

        django_root = 'path_to_your_django_root' # Where manage.py is
        settings_path = 'my_site.settings'

        model = 'Test'
        app = 'test'

        def requires(self):
            return BaseTask()

    if __name__ == '__main__':
        task1, task2 = TestTask(), BaseTask()
        luigi.build([task1, task2], local_scheduler=True)




"""
import os
import sys
import luigi
import django
from django.apps import apps
from django.conf import settings
import abc
import logging


class DjangoTarget(luigi.Target):
    """
    Database target using Django ORM.
    This will rarely have to be directly instantiated by the user.

    Typical usage would be to ovverride `django.ToModel` class
    to manipulate ORM objects.
    """
    marker_model = None

    def __init__(self, django_root, target_model, update_id, echo=False, settings_path='mysite.settings'):

        self.target_model = target_model
        self.django_root = django_root
        self.update_id = update_id
        self.echo = echo
        self.settings_path = settings_path
        self.marker_model_bound = None

    def touch(self):
        """
        Mark this update as complete.
        """
        if self.marker_model_bound is None:
            self.setup_django()

        model = self.marker_model_bound

        ins = model(update_id=self.update_id, target_model=self.target_model)
        ins.save()

        assert self.exists()

    def exists(self):
        row = None
        if self.marker_model_bound is None:
            self.setup_django()

        model = self.marker_model_bound

        row = model.objects.filter(update_id=self.update_id).first()

        return row is not None

    def setup_django(self):
        if self.marker_model is None:
            self.marker_model = luigi.configuration.get_config().get('django', 'marker-model', 'TableUpdates')
        if not apps.ready and not settings.configured:
            sys.path.append(self.django_root)
            os.environ.setdefault("DJANGO_SETTINGS_MODULE", self.settings_path)
            django.setup()
        self.marker_model_bound = apps.get_model('django_luigi', self.marker_model)


class ToORM(luigi.Task):
    """
    An abstract task for inserting data set into Django ORM

    Usage:

    * subclass and override the required `django_root`, `settings_path`, 'app' and `model` attributes.

    Note:
    the model should already exist and migrated before use

    """
    _logger = logging.getLogger('luigi-interface')

    echo = False
    connect_args = {}

    @abc.abstractproperty
    def django_root(self):
        return None

    @abc.abstractproperty
    def settings_path(self):
        return None

    app = None
    model = None
    column_separator = "\t"  # how columns are separated in the file copied into postgres

    def setup_django(self):
        if not apps.ready and not settings.configured:
            sys.path.append(self.django_root)
            os.environ.setdefault("DJANGO_SETTINGS_MODULE", self.settings_path)
            django.setup()

    def update_id(self):
        """
        This update id will be a unique identifier for this insert on this table.
        """
        return self.task_id

    def output(self):
        return DjangoTarget(django_root=self.django_root,
                            target_model=self.model,
                            settings_path=self.settings_path,
                            update_id=self.update_id(),
                            echo=self.echo)

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.

        This method can be overridden for custom file types or formats.
        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip("\n").split(self.column_separator)

    def run(self):
        self._logger.info("Running task copy to ORM for update id %s for table %s" % (self.update_id(), self.model))
        output = self.output()
        self.setup_django()
        model = apps.get_model(self.app, self.model)
        _ = model(**dict(self.rows()))
        _.save()
        output.touch()
        self._logger.info("Finished inserting rows into Django target")
