.. figure:: ./docs/DisdatTitleFig.jpg
   :alt: Disdat Logo
   :align: center

\

.. image:: https://badge.fury.io/py/disdat.svg
    :target: https://badge.fury.io/py/disdat

\
\

Note: Disdat 1.0 no longer contains the instrumented form of Luigi.  Disdat-Luigi now resides `here <https://github.com/kyocum/disdat-luigi>`_.  Want to build versioned pipelines?  ``pip install disdat-luigi``   Want to just use the Disdat API?   ``pip install disdat`` 

Disdat is a Python (3.6, 3.7, +) package for data versioning that allows data scientists to create, share, and track data products.  Disdat organizes data into *bundles*, collections of literal values and files -- bundles are the unit at which data is versioned and shared.   Disdat provides an *API* for creating, finding, and publishing bundles to cloud storage (e.g., AWS S3).  

`Disdat-Luigi  <https://github.com/kyocum/disdat-luigi>`_ uses this API to instrument Spotify's Luigi, so you can build pipelines that automatically create bundles, making it easy to share the latest outputs with other users and pipelines.  Instead of lengthy email conversations with multiple file attachments, searching through Slack for the most recent S3 file path, users can instead ``dsdt pull awesome_data`` to get the latest 'awesome_data.'


Disdat's bundle API and pipelines provide:

* **Simplified pipelines** -- Users implement two functions per task: `requires` and `run`.

* **Enhanced re-execution logic** -- Disdat re-runs processing steps when code or data changes.

* **Data versioning/lineage** -- Disdat records code and data versions for each output data set.

* **Share data sets** -- Users may push and pull data to remote contexts hosted in AWS S3.

* **Auto-docking** -- Disdat *dockerizes* pipelines so that they can run locally or execute on the cloud.

Find our latest documentation on `gitbook here <https://disdat.gitbook.io>`_!


Authors
-------

Disdat could not have come to be without the support of `Human Longevity, Inc. <https://www.humanlongevity.com>`_  It
has benefited from numerous discussions, code contributions, and emotional support from Sean Rowan, Ted Wong, Jonathon Lunt, 
Jason Knight, Axel Bernel, and `Intuit, Inc. <https://www.intuit.com>`_.
