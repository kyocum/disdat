#
# Create a SageMaker Training Container
# Inherit from 02-user.dockerfile, only replace the entrypoint.
#

ARG IMAGE_LAYER
FROM $IMAGE_LAYER

LABEL \
	author="Ken Yocum"

COPY bin/sagemaker.py /opt/bin/sagemaker.py
ENTRYPOINT [ "/opt/bin/sagemaker.py" ]
