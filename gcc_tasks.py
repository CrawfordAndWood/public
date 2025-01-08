import sys, os
from flask import url_for
from app import db, cache
from rq import get_current_job
from rq.utils import import_attribute
from shutil import copyfileobj


try:
	from app.app import sentry
except ImportError:
	sentry = None	


def import_s3_file(file_id, **kwargs):
	job = get_current_job()

	def save_progress(a, b):
		job.meta['n'] = a
		job.meta['length'] = b
		job.save_meta()

	return import_file_s3_worker(file_id, progress_callback=save_progress, **kwargs)


def import_file_s3_worker(file_id, **kwargs):
	from app.models import File
	
	# Thread-local session. scopefunc=None should cause SQLAlchemy-flask not
	# to interfere and let SQLAlchemy figure out a thread-local scope.
	Session = db.create_scoped_session({'scopefunc': None})
	try:
		session = Session()
		file = session.query(File).get(file_id)
		
		file.import_data(session, **kwargs)
		session.commit()
	finally:
		Session.remove()


def download_and_optimize_s3_file(file_id, file_name, s3_name, file_type, source_path, full_path):
  from app.models import File, FileType, LayerFileAssociation, LayerFileAttribute
  from tempfile import TemporaryFile, NamedTemporaryFile

  # download the actual file
  print('gcc tasks, downloading file: ', s3_name, file_name)
  File.download_from_s3(s3_name, source_path)
  
  print('optimization check')
  # check if it needs optimization
  if file_type == FileType.RASTER and geotiff_needs_optimization(full_path):
    print('optimizing')
    upload_file = NamedTemporaryFile(suffix='.tiff', dir=os.path.dirname(source_path))
    convert_to_cloud_optimized_geotiff(full_path, upload_file)
    upload_file.seek(0)
    with open(full_path, 'wb') as dst:
      copyfileobj(upload_file, dst)

    upload_file.seek(0)
    print('optimzation complete')

				
def geotiff_needs_optimization(src_path):
	"""
	Validate Cloud Optimized Geotiff.
	Parameters
	----------
	src_path : str or PathLike object
		A dataset path or URL. Will be opened in "r" mode.
	This script is the rasterio equivalent of
	https://svn.osgeo.org/gdal/trunk/gdal/swig/python/samples/validate_cloud_optimized_geotiff.py

	Source: https://github.com/cogeotiff/rio-cogeo/blob/master/rio_cogeo/cogeo.py
	"""
	from rasterio.env import GDALVersion
	import rasterio

	errors = []
	warnings = []
	details = {}

	if not GDALVersion.runtime().at_least("2.2"):
		raise Exception("GDAL 2.2 or above required")

	config = dict(GDAL_DISABLE_READDIR_ON_OPEN="FALSE")
	with rasterio.Env(**config):
		with rasterio.open(src_path) as src:
			if not src.driver == "GTiff":
				raise Exception("The file is not a GeoTIFF")

			filelist = [os.path.basename(f) for f in src.files]
			src_bname = os.path.basename(src_path)
			if len(filelist) > 1 and src_bname + ".ovr" in filelist:
				errors.append(
					"Overviews found in external .ovr file. They should be internal"
				)

			overviews = src.overviews(1)
			if src.width > 512 or src.height > 512:
				if not src.is_tiled:
					errors.append(
						"The file is greater than 512xH or 512xW, but is not tiled"
					)

				if not overviews:
					warnings.append(
						"The file is greater than 512xH or 512xW, it is recommended "
						"to include internal overviews"
					)

			ifd_offset = int(src.get_tag_item("IFD_OFFSET", "TIFF", bidx=1))
			ifd_offsets = [ifd_offset]
			if ifd_offset not in (8, 16):
				errors.append(
					"The offset of the main IFD should be 8 for ClassicTIFF "
					"or 16 for BigTIFF. It is {} instead".format(ifd_offset)
				)

			details["ifd_offsets"] = {}
			details["ifd_offsets"]["main"] = ifd_offset

			if overviews and overviews != sorted(overviews):
				errors.append("Overviews should be sorted")

			for ix, dec in enumerate(overviews):

				# NOTE: Size check is handled in rasterio `src.overviews` methods
				# https://github.com/mapbox/rasterio/blob/4ebdaa08cdcc65b141ed3fe95cf8bbdd9117bc0b/rasterio/_base.pyx
				# We just need to make sure the decimation level is > 1
				if not dec > 1:
					errors.append(
						"Invalid Decimation {} for overview level {}".format(dec, ix)
					)

				# Check that the IFD of descending overviews are sorted by increasing
				# offsets
				ifd_offset = int(src.get_tag_item("IFD_OFFSET", "TIFF", bidx=1, ovr=ix))
				ifd_offsets.append(ifd_offset)

				details["ifd_offsets"]["overview_{}".format(ix)] = ifd_offset
				if ifd_offsets[-1] < ifd_offsets[-2]:
					if ix == 0:
						errors.append(
							"The offset of the IFD for overview of index {} is {}, "
							"whereas it should be greater than the one of the main "
							"image, which is at byte {}".format(
								ix, ifd_offsets[-1], ifd_offsets[-2]
							)
						)
					else:
						errors.append(
							"The offset of the IFD for overview of index {} is {}, "
							"whereas it should be greater than the one of index {}, "
							"which is at byte {}".format(
								ix, ifd_offsets[-1], ix - 1, ifd_offsets[-2]
							)
						)

			block_offset = int(src.get_tag_item("BLOCK_OFFSET_0_0", "TIFF", bidx=1))
			if not block_offset:
				errors.append("Missing BLOCK_OFFSET_0_0")

			data_offset = int(block_offset) if block_offset else None
			data_offsets = [data_offset]
			details["data_offsets"] = {}
			details["data_offsets"]["main"] = data_offset

			for ix, dec in enumerate(overviews):
				data_offset = int(
					src.get_tag_item("BLOCK_OFFSET_0_0", "TIFF", bidx=1, ovr=ix)
				)
				data_offsets.append(data_offset)
				details["data_offsets"]["overview_{}".format(ix)] = data_offset

			if data_offsets[-1] < ifd_offsets[-1]:
				if len(overviews) > 0:
					errors.append(
						"The offset of the first block of the smallest overview "
						"should be after its IFD"
					)
				else:
					errors.append(
						"The offset of the first block of the image should "
						"be after its IFD"
					)

			for i in range(len(data_offsets) - 2, 0, -1):
				if data_offsets[i] < data_offsets[i + 1]:
					errors.append(
						"The offset of the first block of overview of index {} should "
						"be after the one of the overview of index {}".format(i - 1, i)
					)

			if len(data_offsets) >= 2 and data_offsets[0] < data_offsets[1]:
				errors.append(
					"The offset of the first block of the main resolution image "
					"should be after the one of the overview of index {}".format(
						len(overviews) - 1
					)
				)

		for ix, dec in enumerate(overviews):
			with rasterio.open(src_path, OVERVIEW_LEVEL=ix) as ovr_dst:
				if ovr_dst.width >= 512 or ovr_dst.height >= 512:
					if not ovr_dst.is_tiled:
						errors.append("Overview of index {} is not tiled".format(ix))

	return warnings or errors

def convert_to_cloud_optimized_geotiff(path, file):
	from rio_cogeo.cogeo import cog_translate
	from rio_cogeo.profiles import cog_profiles

	print('converting to cloud optimized geotiff: ', file)
	profile = cog_profiles.get('deflate')
	cog_translate(
		path,
		file.name if hasattr(file, 'name') else file,
		{
			**profile,
			'BIGTIFF': 'IF_SAFER'
		},
		config={
			'GDAL_TIFF_OVR_BLOCKSIZE': '128',
			'GDAL_NUM_THREADS': 'ALL_CPUS',
			'GDAL_TIFF_INTERNAL_MASK': True
		})

