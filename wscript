import Options
from os.path import exists
from shutil import copy2 as copy

TARGET = 'bplus'
TARGET_FILE = '%s.node' % TARGET
built = 'build/Release/%s' % TARGET_FILE
dest = 'lib/bplus/%s' % TARGET_FILE

def set_options(opt):
  opt.tool_options("compiler_cxx")

def configure(conf):
  conf.check_tool("compiler_cxx")
  conf.check_tool("node_addon")

def pre(ctx):
  if Options.platform == "darwin":
    ctx.exec_command('make ARCH=i386 MODE=RELEASE -C ../deps/bplus/')
  else:
    ctx.exec_command('make MODE=RELEASE -C ../deps/bplus/')

def build(bld):
  bld.add_pre_fun(pre)
  obj = bld.new_task_gen("cxx", "shlib", "node_addon")
  if Options.platform == "darwin":
    obj.cxxflags = ["-g", "-D_LARGEFILE_SOURCE", "-Wall", "-arch", "i386"]
    obj.ldflags = ["-arch", "i386"]
    obj.env['DEST_CPU'] = 'i386'
  else:
    obj.cxxflags = ["-g", "-D_LARGEFILE_SOURCE", "-Wall"]
  obj.env.LINKFLAGS = ["../deps/bplus/bplus.a"]
  obj.target = TARGET
  obj.source = "src/node_bplus.cc"
  obj.includes = "src/ deps/bplus/include"

def shutdown():
  if Options.commands['clean']:
      if exists(TARGET_FILE):
        unlink(TARGET_FILE)
  else:
    if exists(built):
      copy(built, dest)
