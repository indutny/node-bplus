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
    ctx.exec_command('make -B ARCH=i386 MODE=RELEASE -C ../deps/bplus/')
  else:
    ctx.exec_command('make -B MODE=RELEASE -C ../deps/bplus/')

def build(bld):
  bld.add_pre_fun(pre)
  obj = bld.new_task_gen("cxx", "shlib", "node_addon")
  obj.cxxflags = ["-g", "-D_LARGEFILE_SOURCE", "-Wall"]
  obj.ldflags = ["../deps/bplus/bplus.a"]
  if Options.platform == "darwin":
    obj.cxxflags.append("-arch")
    obj.cxxflags.append("i386")
    obj.ldflags.append("-arch")
    obj.ldflags.append("i386")
    obj.env['DEST_CPU'] = 'i386'
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
