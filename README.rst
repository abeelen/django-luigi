=====
luigi
=====

Luigi is a simple Django app to be able to link Django to the workflow manager luigi.

Quick start
-----------

1. Add 'django_luigi' to you INSTALLED_APPS settings like this::

     INSTALLED_APP = [...
     'django_luigi',
     ]

2. Create your `app` and `models` in Django as usual

3. Run `python manage.py migrate` to create the models.
 
You can know create objects in your luigi pipeline.

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
