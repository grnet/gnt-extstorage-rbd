#!/usr/bin/env python

# Copyright (C) 2016 GRNET S.A.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.

"""
RBD storage provider wrapper-script for ganeti extstorage disk template

The script takes it's input from environment variables. Specifically the
following variables should be present:

 - VOL_CNAME: The name of the new Image file
 - VOL_SIZE: The size of the new Image (in megabytes)

The following variables are optional:

 - EXTP_ORIGIN: The name of the Image file to snapshot
 - EXTP_REUSE_DATA: An indication to RBD that it should not create a new volume
   but reuse an existing one
 - EXTP_RBD_POOL: The pool that the RBD volume resides
 - EXTP_CEPHX_ID Specifies the username (without the client. prefix) to use
   with the map command
 - EXTP_CEPHX_KEYRING Specifies a keyring file containing a secret for the
   specified user to use with the map command
 - EXTP_CEPHX_KEYFILE Specifies a file containing the secret key of --id user
   to use with the map command
 - EXTP_IMAGE_FORMAT: The image format of the new RBD volume
 - EXTP_IMAGE_FEATURES: The enabled features of the new RBD volume
 - EXTP_STRIPE_UNIT Size (in bytes) of a block of data
 - EXTP_STRIPE_COUNT Number of consecutive objects in a stripe
 - EXTP_USERSPACE_ONLY Number of consecutive objects in a stripe

The code branches to the correct function, depending on the name (sys.argv[0])
of the executed script (attach, create, etc).

Returns O after successful completion, 1 on failure

"""

import os
import sys
import subprocess
import json
import re

TRUE_PATTERN = '^(yes|true|on|1|set)$'
PREFIX_EXTP = 'EXTP_'


def cmd_open(cmd, bufsize=-1, env=None):
    inst = subprocess.Popen(cmd, shell=False, bufsize=bufsize,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, close_fds=True)
    return inst


def doexec(args, inputtext=None):
    proc = cmd_open(args)
    if inputtext is not None:
        proc.stdin.write(inputtext)
    stdout = proc.stdout
    stderr = proc.stderr
    rc = proc.wait()
    return (rc, stdout, stderr)


class RBDException(Exception):
    pass


class RBD(object):
    RBD_CMD = 'rbd'

    @staticmethod
    def format_name(name, pool=None, snapshot=None):
        image_name = name
        if pool is not None:
            image_name = pool + '/' + image_name
        if snapshot is not None:
            image_name = image_name + '@' + snapshot
        return image_name

    @staticmethod
    def _exc(args):
        rc, stdout, stderr = doexec([RBD.RBD_CMD] + args)
        out, err = stdout.read().strip(), stderr.read().strip()
        stdout.close()
        stderr.close()
        if rc:
            raise RBDException('%s failed (%s %s %s)' %
                               (args, rc, out, err))
        return out

    @staticmethod
    def exc(cephx, *args):
        args = list(args)
        if cephx:
            cephx_args = []
            if cephx.get('id') is not None:
                id = str(cephx.get('id'))
                cephx_args.append('--id')
                cephx_args.append(id)
                sys.stderr.write("Using cephx id %s\n" % id)
            if cephx.get('keyring') is not None:
                keyring = str(cephx.get('keyring'))
                cephx_args.append('--keyring')
                cephx_args.append(keyring)
                sys.stderr.write("Using cephx keyring %s\n" % keyring)
            if cephx.get('keyfile') is not None:
                keyfile = str(cephx.get('keyfile'))
                cephx_args.append('--keyfile')
                cephx_args.append(keyfile)
                sys.stderr.write("Using cephx keyfile %s\n" % keyfile)

            args = cephx_args + args

        return RBD._exc(args)

    @staticmethod
    def list(pool=None, cephx=None):
        mappings = json.loads(RBD.exc(cephx, 'showmapped', '--format', 'json'))
        if pool:
            return {k: v for k, v in mappings.iteritems() if v['pool'] == pool}
        else:
            return mappings

    @staticmethod
    def get_device(image, pool=None, cephx=None):
        """ Return the device the image is mapped else None"""
        list = RBD.list(pool=pool, cephx=cephx)
        for mapping in list.itervalues():
            if mapping['name'] == image:
                return mapping['device']

        return None

    @staticmethod
    def create(image, size, pool=None, image_format=None, image_features=None,
               stripe_unit=None, stripe_count=None, cephx=None):
        """ Map an image to an RBD device """

        image = RBD.format_name(image, pool=pool)

        args = []
        if image_format is not None:
            args.append('--image-format')
            args.append(str(image_format))
        if image_features is not None:
            args.append('--image-features')
            args.append(str(image_features))
        if stripe_unit is not None:
            args.append('--stripe-unit')
            args.append(str(stripe_unit))
        if stripe_count is not None:
            args.append('--stripe-count')
            args.append(str(stripe_count))

        return RBD.exc(cephx, 'create', image, '--size', str(size), *args)

    @staticmethod
    def map(image, pool=None, cephx=None):
        """ Map an image to an RBD device """
        image = RBD.format_name(image, pool=pool)
        return RBD.exc(cephx, 'map', image)

    @staticmethod
    def unmap(device, cephx=None):
        """ Unmap an RBD device """
        return RBD.exc(cephx, 'unmap', device)

    @staticmethod
    def resize(image, size, pool=None, cephx=None):
        """ Unmap an RBD device """
        image = RBD.format_name(image, pool=pool)
        return RBD.exc(cephx, 'resize', image, '--size', size)

    @staticmethod
    def remove(image, pool=None, cephx=None):
        """ Remove an RBD image """
        image = RBD.format_name(image, pool=pool)
        return RBD.exc(cephx, 'rm', image)


def read_env():
    """Read the enviromental variables"""
    name = os.getenv("VOL_CNAME")
    if name is None:
        sys.stderr.write('The environment variable VOL_CNAME is missing.\n')
        return None

    extp_params = {}
    for k, v in os.environ.iteritems():
        if k.startswith(PREFIX_EXTP):
            extp_params[k[len(PREFIX_EXTP):].lower()] = v

    reuse_data = False
    if extp_params.get("reuse_data"):
        reuse_data = re.match(TRUE_PATTERN, os.getenv("EXTP_REUSE_DATA"),
                              flags=re.IGNORECASE) is not None
        extp_params.pop("reuse_data")

    userspace_only = False
    if extp_params.get("userspace_only"):
        userspace_only = re.match(
            TRUE_PATTERN, os.getenv("EXTP_USERSPACE_ONLY"),
            flags=re.IGNORECASE) is not None
        extp_params.pop("userspace_only")

    cephx_keys = ['cephx_id', 'cephx_keyring', 'cephx_keyfile']
    cephx = {}
    for k in cephx_keys:
        param = extp_params.get(k)
        if param:
            cephx[k[len('cephx_'):]] = param
            extp_params.pop(k)

    env = {"name": os.getenv("VOL_CNAME"),
           "size": os.getenv("VOL_SIZE"),
           "snapshot_name": os.getenv("VOL_SNAPSHOT_NAME"),
           "cephx": cephx,
           "reuse_data": reuse_data,
           "userspace_only": userspace_only
           }
    env.update(extp_params)
    return env


def create(env):
    """Create a new RBD Image"""
    name = env.get("name")
    size = env.get("size")
    origin = env.get("origin")
    reuse_data = env.get("reuse_data")
    pool = env.get("rbd_pool")
    image_format = env.get("image_format")
    image_features = env.get("image_features")
    stripe_unit = env.get("stripe_unit")
    stripe_count = env.get("stripe_count")
    cephx = env.get("cephx")

    if reuse_data:
        sys.stderr.write("Reusing previous data for %s\n"
                         % RBD.format_name(name, pool=pool))
        return 0

    if origin:
        sys.stderr.write("Cloning is not supported yet\n")
        return 1
    else:
        sys.stderr.write("Creating volume '%s' of size '%s'\n"
                         % (RBD.format_name(name, pool=pool), size))
        RBD.create(name, size, pool=pool, image_format=image_format,
                   image_features=image_features, stripe_unit=stripe_unit,
                   stripe_count=stripe_count, cephx=cephx)
    return 0


def snapshot(env):
    """Create a snapshot of an existing RBD Image."""
    # name = env.get("name")
    # snapshot_name = env.get("snapshot_name")
    # sys.stderr.write("Creating snapshot '%s' from '%s'\n" %
    #                  (snapshot_name, name))
    # RBD.snapshot(name, snapshot_name)
    # return 0
    sys.stderr.write("RBD snapshot is not supported yet")
    return 1


def format_qemu_uri(name, pool=None, cephx=None, conf_file=None):
    """Create a QEMU RBD URI for the specific image / environment"""

    uri = 'kvm:rbd:%s' % RBD.format_name(name, pool=pool)
    extra_conf = ''
    if cephx['id']:
        extra_conf += ':id=%s' % cephx['id']
    if conf_file:
        extra_conf += ':conf=%s' % conf_file

    if extra_conf:
        uri += extra_conf

    return uri


def attach(env):
    """
    Map an existing RBD Image to a block device

    This is an idempotent function that maps an existing RBD Image to a block
    device e.g. /dev/rbd{X} and returns the device path. If the mapping already
    exists, it returns the corresponding device path.

    """

    userspace_only = env.get("userspace_only")
    name = env.get("name")
    pool = env.get("rbd_pool")
    cephx = env.get("cephx")
    if userspace_only:
        device = ""
    else:
        device = RBD.get_device(name)
        cephx = env.get("cephx")
        if device is None:
            device = RBD.map(name, pool=pool, cephx=cephx)
            sys.stderr.write("Mapped image '%s' to '%s' \n"
                             % (RBD.format_name(name, pool=pool), device))
        else:
            sys.stderr.write("Image '%s' already mapped to device '%s' \n"
                             % (RBD.format_name(name, pool=pool), device))

    sys.stdout.write("%s" % device)
    qemu_uri = format_qemu_uri(name, pool=pool, cephx=cephx)
    sys.stdout.write("\n%s" % qemu_uri)

    return 0


def detach(env):
    """
    Unmap an RBD device from the host.

    This is an idempotent function that unmaps an RBD device from the host.
    If mapping doesn't exist at all, it does nothing.

    """
    userspace_only = env.get("userspace_only")
    if not userspace_only:
        name = env.get("name")
        pool = env.get("rbd_pool")
        cephx = env.get("cephx")
        device = RBD.get_device(name, pool=pool)
        if device:
            RBD.unmap(device, cephx=cephx)

        sys.stderr.write("Unmapped %s\n" % RBD.format_name(name, pool=pool))

    return 0


def grow(env):
    """Grow an existing RBD Image"""
    name = env.get("name")
    size = env.get("size")
    pool = env.get("rbd_pool")
    cephx = env.get("cephx")

    sys.stderr.write("Resizing '%s'. New size '%s'\n"
                     % (RBD.format_name(name, pool=pool), size))
    RBD.resize(name, size, pool=pool, cephx=cephx)
    return 0


def remove(env):
    """
    Delete an RBD Image.

    This deletes all blocks of an RBD image and can take some time to complete
    for larger images.
    """
    name = env.get("name")
    pool = env.get("rbd_pool")
    cephx = env.get("cephx")
    sys.stderr.write("Deleting '%s'\n" % RBD.format_name(name, pool=pool))
    RBD.remove(name, pool=pool, cephx=cephx)
    return 0


def verify(env):
    return 0


def setinfo(env):
    return 0


def main():
    env = read_env()
    if env is None:
        sys.stderr.write("Wrong environment. Aborting...\n")
        return 1

    actions = {
        'create': create,
        'snapshot': snapshot,
        'attach': attach,
        'detach': detach,
        'grow': grow,
        'remove': remove,
        'verify': verify,
        'setinfo': setinfo,
    }

    try:
        action_name = os.path.basename(sys.argv[0])
        action = actions[action_name]
    except KeyError:
        sys.stderr.write("Action '%s' not supported\n" % action_name)
        return 1

    try:
        return action(env)
    except RBDException as e:
        sys.stderr.write("RBD command error: %s\n" % e)
        return 1
    except Exception as e:
        # Log all exceptions here and return error
        import traceback
        trace = traceback.format_exc()
        sys.stderr.write("Error: %s\n" % e)
        sys.stderr.write("Trace: %s\n" % trace)
        return 1


if __name__ == "__main__":
    sys.exit(main())
