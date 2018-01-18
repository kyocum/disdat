# -*- mode: python -*-

block_cipher = None

added_files = [ ('disdat', 'disdat'),
('setup.py','.'),
('infrastructure/dockerizer', 'infrastructure/dockerizer'),
('infrastructure/__init__.py', 'infrastructure/.')]


a = Analysis(['disdat/dsdt.py'],
             pathex=['/Users/kyocum/Code/disdat'],
             binaries=[],
             datas=added_files,
             hiddenimports=["disdat.api"],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='dsdt',
          debug=False,
          strip=False,
          upx=True,
          runtime_tmpdir=None,
          console=True )
