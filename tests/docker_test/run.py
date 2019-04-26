import argparse
import os


PIPES_CLASS         = os.environ.get("PIPES_CLASS", None)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Disdat Calling Convention Parsing')
    parser.add_argument('--output-bundle', type=str, help='output bundle', required=True)

    parser.add_argument('--remote', type=str, help='Workflow execution id', required=True, default=None)
    parser.add_argument('--pipeline', type=str, help='module.Class', required=(PIPES_CLASS is None), default=PIPES_CLASS)

    parser.add_argument('--branch', type=str, help='Branch off of data context', required=True)
    parser.add_argument('--output-bundle-uuid', type=str, help='UUID for final output bundle', required=True)
    parser.add_argument('--force', action='store_true', help='force recomputation', default=False)


    parser.add_argument("pipeline_args", nargs=argparse.REMAINDER, type=str,
                        help='One or more optional arguments to pass on to the pipeline class')

    args, _ = parser.parse_known_args()

    print "PIPES DOCKERIZED"
    print "Args are:"
    print "output bundle: {}".format(args.output_bundle)
    print "pipe_class: {}".format(args.pipeline)
    print "branch: {}".format(args.branch)
    print "remote: {}".format(args.remote)
    print "force: {}".format(args.force)
    print "uuid: {}".format(args.output_bundle_uuid)

    print "KWARGS: {}".format(args.pipeline_args)


