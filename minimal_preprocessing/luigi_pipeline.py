import luigi
from pathlib import Path
import shlex
from luigi.contrib.external_program import ExternalProgramTask


class Refit(ExternalProgramTask):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(str(self.filepath))

    def program_args(self):
        return ["3drefit", "-deoblique", self.filepath]


class Resample(ExternalProgramTask):
    filepath = luigi.Parameter()
    outpath = None

    def output(self):
        outpath = add_name(self.filepath, "resample")
        self.outpath = outpath
        return luigi.LocalTarget(outpath)

    def requires(self):
        return Refit(self.filepath)

    def program_args(self):
        return ["3dresample", "-orient", "RPI", "-prefix", self.outpath, "-inset", self.filepath]

class MeanImage(ExternalProgramTask):
    filepath = luigi.Parameter()
    outpath_string = None
    def requires(self):
        return Resample(self.filepath)

    def output(self):
        self.outpath_string = add_name(str(self.input().path), "mean")
        return luigi.LocalTarget(self.outpath_string)

    def program_args(self):
        inpath = str(self.input().path)
        return ["3dTstat", "-mean", "-prefix", self.outpath_string, inpath]

class MotionCorrect(ExternalProgramTask):
    filepath = luigi.Parameter()

    def requires(self):
        return Resample(self.filepath), MeanImage(self.filepath)

    def output(self):
        image, mean = self.input()
        name = str(image.path).split(".")[0]

        output = {
            "aff12": luigi.LocalTarget(name + "_aff12.1D"),
            "output1D": luigi.LocalTarget(name + "_resample.1D"),
            "maxdisp1D": luigi.LocalTarget(name + "_max_displacement.1D"),
            "output_image": luigi.LocalTarget(name + "_volreg.nii.gz")
        }
        return output

    def program_args(self):
        image, mean = self.input()
        out_dict = self.output()
        return shlex.split(
            "3dvolreg -Fourier -twopass -1Dfile {output1D} "
            "-1Dmatrix_save {output_affine} "
            "-prefix {outpath} "
            "-base {meanpath} "
            "-zpad 4 "
            "-maxdisp1D {output_max_displacement} {filepath}".format(output1D=str(out_dict["output1D"].path),
                                                                     output_affine=str(out_dict["aff12"].path),
                                                                     outpath=str(out_dict["output_image"].path),
                                                                     meanpath=str(mean.path),
                                                                     output_max_displacement=str(out_dict["maxdisp1D"].path),
                                                                     filepath=str(image.path)
                                                                     )
        )

class MeanMotionCorrect(MeanImage):
    def requires(self):
        return MotionCorrect(self.filepath)
    def output(self):
        self.outpath_string = add_name(str(self.input()["output_image"].path), "mean")
        return luigi.LocalTarget(self.outpath_string)


class SkullStrip(ExternalProgramTask):
    filepath = luigi.Parameter()

    def requires(self):
        return Resample(self.filepath)

    def output(self):
        return luigi.LocalTarget(add_name(str(self.input().path), "skullstrip"))

    def program_args(self):
        outpath = str(self.output().path)
        inpath = str(self.input().path)
        return ["3dSkullStrip", "-orig_vol", "-prefix", outpath, "-input", inpath]


class AntsRegister(ExternalProgramTask):
    filepath = luigi.Parameter()
    input_folder = None

    def requires(self):
        return SkullStrip(self.filepath)

    def output(self):
        input_path = str(self.input().path)
        input_folder = Path(input_path).parents[0]
        self.input_folder = input_folder
        output = {
            "transform0": input_folder/"transform0DerivedInitialMovingTranslation.mat",
            "transform1": input_folder/"transform1Rigid.mat",
            "transform2": input_folder/"transform2Affine.mat",
            "transform3": input_folder/"transform3Warp.nii.gz"
        }

        return output

    def program_args(self):
        inpath = str(self.input().path)
        return shlex.split(
            "antsRegistration "
            "--collapse-output-transforms 0 "
            "--dimensionality 3 "
            "--initial-moving-transform [/usr/share/fsl/5.0/data/standard/MNI152_T1_2mm_brain.nii.gz,"
            "{filepath},0] "
            "--interpolation Linear "
            "--output [{outpath}/transform,{outpath}/transform_Warped.nii.gz] "
            "--transform Rigid[0.1] "
            "--metric MI[/usr/share/fsl/5.0/data/standard/MNI152_T1_2mm_brain.nii.gz,"
            "{filepath},1,32,Regular,0.25] "
            "--convergence [1000x500x250x100,1e-08,10] "
            "--smoothing-sigmas 3.0x2.0x1.0x0.0 "
            "--shrink-factors 8x4x2x1 "
            "--use-histogram-matching 1 "
            "--transform Affine[0.1] "
            "--metric MI[/usr/share/fsl/5.0/data/standard/MNI152_T1_2mm_brain.nii.gz,"
            "{filepath},1,32,Regular,0.25] "
            "--convergence [1000x500x250x100,1e-08,10] "
            "--smoothing-sigmas 3.0x2.0x1.0x0.0 "
            "--shrink-factors 8x4x2x1 "
            "--use-histogram-matching 1 "
            "--transform SyN[0.1,3.0,0.0] "
            "--metric CC[/usr/share/fsl/5.0/data/standard/MNI152_T1_2mm_brain.nii.gz,"
            "{filepath},1,4] "
            "--convergence [100x100x70x20,1e-09,15] "
            "--smoothing-sigmas 3.0x2.0x1.0x0.0 "
            "--shrink-factors 6x4x2x1 "
            "--use-histogram-matching 1 "
            "--winsorize-image-intensities [0.01,0.99] -v".format(filepath=inpath, outpath=self.input_folder))



def add_name(filepath, task_name = "task"):
    filepath_string = str(filepath)
    outpath = ".".join([filepath_string.split(".")[0] + "_" + task_name, filepath_string.split(".", 1)[1]])
    return outpath

if __name__ == "__main__":
    luigi.run()
