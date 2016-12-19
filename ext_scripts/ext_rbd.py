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
    def exc(*args):
        rc, stdout, stderr = doexec([RBD.RBD_CMD] + list(args))
        out, err = stdout.read().strip(), stderr.read().strip()
        stdout.close()
        stderr.close()
        if rc:
            raise RBDException('%s failed (%s %s %s)' %
                               (args, rc, out, err))
        return out

    @staticmethod
    def list():
        return json.loads(RBD.exc('showmapped', '--format', 'json'))

    @staticmethod
    def get_device(image):
        """ Return the device the image is mapped else None"""
        list = RBD.list()
        for mapping in list.itervalues():
            if mapping['name'] == image:
                return mapping['device']

        return None

    @staticmethod
    def create(image, size, image_format=None, image_features=None):
        """ Map an image to an RBD device """
        args = []
        if image_format is not None:
            args.append('--image_format')
            args.append(image_format)
        if image_features is not None:
            args.append('--image_features')
            args.append(image_features)

        return RBD.exc("create", image, '--size', str(size), *args)

    @staticmethod
    def map(image):
        """ Map an image to an RBD device """
        return RBD.exc("map", image)

    @staticmethod
    def unmap(device):
        """ Unmap an RBD device """
        return RBD.exc("unmap", device)

    @staticmethod
    def resize(image, size):
        """ Unmap an RBD device """
        return RBD.exc("resize", image, "--size", size)

    @staticmethod
    def remove(image):
        """ Remove an RBD image """
        return RBD.exc("rm", image)


def read_env():
    """Read the enviromental variables"""
    name = os.getenv("VOL_CNAME")
    if name is None:
        sys.stderr.write('The environment variable VOL_CNAME is missing.\n')
        return None

    reuse_data = False
    if os.getenv("EXTP_REUSE_DATA"):
        reuse_data = re.match(TRUE_PATTERN, os.getenv("EXTP_REUSE_DATA"),
                              flags=re.IGNORECASE) is not None

    return {"name": name,
            "size": os.getenv("VOL_SIZE"),
            "origin": os.getenv("EXTP_ORIGIN"),
            "snapshot_name": os.getenv("VOL_SNAPSHOT_NAME"),
            "reuse_data": reuse_data,
            }


def create(env):
    """Create a new RBD Image"""
    name = env.get("name")
    size = env.get("size")
    origin = env.get("origin")
    reuse_data = env.get("reuse_data")

    if reuse_data:
        sys.stderr.write("Reusing previous data for %s\n" % name)
        return 0

    if origin:
        sys.stderr.write("Cloning is not supported yet\n")
        return 1
    else:
        sys.stderr.write("Creating volume '%s' of size '%s'\n"
                         % (name, size))
        RBD.create(name, size)
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


def attach(env):
    """
    Map an existing RBD Image to a block device

    This is an idempotent function that maps an existing RBD Image to a block
    device e.g. /dev/rbd{X} and returns the device path. If the mapping already
    exists, it returns the corresponding device path.

    """

    name = env.get("name")
    device = RBD.get_device(name)
    if device is None:
        device = RBD.map(name)
        sys.stderr.write("Mapped image '%s' to '%s' \n" % (name, device))
    else:
        sys.stderr.write("Image '%s' already mapped to device '%s' \n"
                         % (name, device))

    sys.stdout.write("%s" % device)
    return 0


def detach(env):
    """
    Unmap an RBD device from the host.

    This is an idempotent function that unmaps an RBD device from the host.
    If mapping doesn't exist at all, it does nothing.

    """
    name = env.get("name")
    device = RBD.get_device(name)
    if device:
        RBD.unmap(device)

    sys.stderr.write("Unmapped %s\n" % name)
    return 0


def grow(env):
    """Grow an existing RBD Image"""
    name = env.get("name")
    size = env.get("size")

    sys.stderr.write("Resizing '%s'. New size '%s'\n" % (name, size))
    RBD.resize(name, size)
    return 0


def remove(env):
    """
    Delete an RBD Image.

    This deletes all blocks of an RBD image and can take some time to complete
    for larger images.
    """
    name = env.get("name")
    sys.stderr.write("Deleting '%s'\n" % name)
    RBD.remove(name)
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
