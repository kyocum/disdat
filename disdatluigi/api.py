#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import warnings

import disdat.fs
import disdat.common as common
from disdat.api import _get_context

import disdatluigi.apply
import disdatluigi.run
from disdatluigi.run import Backend, run_entry
from disdatluigi.dockerize import dockerize_entry
from disdatluigi import logger as _logger


def apply(local_context, transform, output_bundle='-',
          input_tags=None, output_tags=None, force=False,
          force_all=False, params=None,
          output_bundle_uuid=None, central_scheduler=False, workers=1,
          incremental_push=False, incremental_pull=False):
    """ Execute a Disdat pipeline natively on the local machine.   Note that `api.run` will execute
    a Disdat pipeline that has been dockerized (either locally or remotely on AWS Batch or AWS Sagemaker)

    Args:
        local_context (str):  The name of the local context in which the pipeline will run in the container
        transform (type[disdat.pipe.PipeTask]): A reference to the Disdat Pipe class
        output_bundle (str):  The name of the output bundle.  Defaults to `<task_name>_<param_hash>`
        input_tags: optional tags dictionary for selecting input bundle
        output_tags: optional tags dictionary to tag output bundle
        force (bool): Force re-running this transform, default False
        force_all (bool): Force re-running ALL transforms, default False
        params: optional parameters dictionary
        output_bundle_uuid: Force UUID of output bundle
        central_scheduler (bool): Use a central scheduler, default False, i.e., use local scheduler
        workers (int): Number of workers, default 1.
        incremental_push (bool): commit and push task bundles as they complete
        incremental_pull (bool): localize bundles from remote as they are required by downstream tasks

    Returns:
        result (int):  0 success, >0 if issue

    """

    # check for deprecated str input for transform
    if isinstance(transform, str):
        msg = ('PipeTask classes should be passed as references, not strings, '
               'support for string inputs will be removed in future versions')
        warnings.warn(msg, DeprecationWarning)
        transform = common.load_class(transform)

    data_context = _get_context(local_context)

    if input_tags is None:
        input_tags = {}

    if output_tags is None:
        output_tags = {}

    if params is None:
        params = {}

    # IF apply raises, let it go up.
    # If API, caller can catch.
    # If CLI, python will exit 1
    result = disdat.apply.apply(output_bundle, params, transform,
                                input_tags, output_tags, force, force_all,
                                output_bundle_uuid=output_bundle_uuid,
                                central_scheduler=central_scheduler,
                                workers=workers,
                                data_context=data_context,
                                incremental_push=incremental_push,
                                incremental_pull=incremental_pull)

    # If no raise, but luigi says not successful
    # If API (here), then raise for caller to catch.
    # For CLI, we exit with 1
    common.apply_handle_result(result, raise_not_exit=True)

    return result


def run(setup_dir,
        local_context,
        pipe_cls,
        pipeline_args=None,
        output_bundle='-',
        remote_context=None,
        remote_s3_url=None,
        backend=Backend.default(),
        input_tags=None,
        output_tags=None,
        force=False,
        force_all=False,
        pull=None,
        push=None,
        no_push_int=False,
        vcpus=2,
        memory=4000,
        workers=1,
        no_submit=False,
        aws_session_token_duration=42300,
        job_role_arn=None):
    """ Execute a pipeline in a container.  Run locally, on AWS Batch, or AWS Sagemaker

    Simplest execution is with a setup directory (that contains your setup.py), the local context in which to
    execute, and the pipeline to run.   By default this call runs the container locally, reading and writing data only
    to the local context.

    By default this call will assume the remote_context and remote_s3_url of the local context on this system.
    Note that the user must provide both the remote_context and remote_s3_url to override the remote context bound
    to the local context (if any).

    Args:
        setup_dir (str): The directory that contains the setup.py holding the requirements for any pipelines
        local_context (str): The name of the local context in which the pipeline will run in the container
        pipe_cls (str): The pkg.module.class of the root of the pipeline DAG
        pipeline_args (dict): Dictionary of the parameters of the root task
        output_bundle (str): The human name of output bundle
        remote_context (str): The remote context to pull / push bundles during execution. Default is `local_context`
        remote_s3_url (str): The remote's S3 path
        backend : Backend.Local | Backend.AWSBatch. Default Backend.local
        input_tags (dict): str:str dictionary of tags required of the input bundle
        output_tags (dict): str:str dictionary of tags placed on all output bundles (including intermediates)
        force (bool):  Re-run the last pipe task no matter prior outputs
        force_all (bool):  Re-run the entire pipeline no matter prior outputs
        pull (bool): Pull before execution. Default if Backend.Local then False, else True
        push (bool): Push output bundles to remote. Default if Backend.Local then False, else True
        no_push_int (bool):  Do not push intermediate task bundles after execution. Default False
        vcpus (int): Number of virtual CPUs (if backend=`AWSBatch`). Default 2.
        memory (int): Number of MB (if backend='AWSBatch'). Default 2000.
        workers (int):  Number of Luigi workers. Default 1.
        no_submit (bool): If True, just create the AWS Batch Job definition, but do not submit the job
        aws_session_token_duration (int): Seconds lifetime of temporary token (backend='AWSBatch'). Default 42300
        job_role_arn (str): AWS ARN for job execution in a batch container (backend='AWSBatch')

    Returns:
        json (str):

    """

    pipeline_arg_list = []
    if pipeline_args is not None:
        for k,v in pipeline_args.items():
            pipeline_arg_list.append(k)
            pipeline_arg_list.append(json.dumps(v))

    # Set up context as 'remote_name/local_name'
    if remote_context is None:
        assert remote_s3_url is None, "disdat.api.run: user must specify both remote_s3_url and remote_context"
        context = local_context
    else:
        assert remote_s3_url is not None, "disdat.api.run: user must specify both remote_s3_url and remote_context"
        context = "{}/{}".format(remote_context, local_context)

    retval = run_entry(output_bundle=output_bundle,
                       pipeline_root=setup_dir,
                       pipeline_args=pipeline_arg_list,
                       pipe_cls=pipe_cls,
                       backend=backend,
                       input_tags=input_tags,
                       output_tags=output_tags,
                       force=force,
                       force_all=force_all,
                       context=context,
                       remote=remote_s3_url,
                       pull=pull,
                       push=push,
                       no_push_int=no_push_int,
                       vcpus=vcpus,
                       memory=memory,
                       workers=workers,
                       no_submit=no_submit,
                       job_role_arn=job_role_arn,
                       aws_session_token_duration=aws_session_token_duration)

    return retval


def dockerize(setup_dir,
              config_dir=None,
              build=True,
              push=False,
              sagemaker=False):
    """ Create a docker container image using a setup.py and pkg.module.class description of the pipeline.

    Note:
        Users set the os_type and os_version in the disdat.cfg file.
        os_type: The base operating system type for the Docker image
        os_version: The base operating system version for the Docker image

    Args:
        setup_dir (str): The directory that contains the setup.py holding the requirements for any pipelines
        config_dir (str): The directory containing the configuration of .deb packages
        build (bool): If False, just copy files into the Docker build context without building image.
        push (bool):  Push the container to the repository
        sagemaker (bool): Create a Docker image executable as a SageMaker container (instead of a local / AWSBatch container).

    Returns:
        (int): 0 if success, 1 on failure

    """

    retval = dockerize_entry(pipeline_root=setup_dir,
                             config_dir=config_dir,
                             os_type=None,
                             os_version=None,
                             build=build,
                             push=push,
                             sagemaker=sagemaker
                             )

    return retval


def dockerize_get_id(setup_dir):
    """ Retrieve the docker container image identifier

    Args:
        setup_dir (str): The directory that contains the setup.py holding the requirements for any pipelines

    Returns:
        (str): The full docker container image hash
    """
    return dockerize_entry(pipeline_root=setup_dir, get_id=True)
