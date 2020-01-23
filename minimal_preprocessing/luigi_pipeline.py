import luigi
from pathlib import Path
import shlex
import os
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
        return {"output_image": luigi.LocalTarget(outpath)}

    def requires(self):
        return Refit(self.filepath)

    def program_args(self):
        return [
            "3dresample",
            "-orient",
            "RPI",
            "-prefix",
            self.outpath,
            "-inset",
            self.filepath,
        ]


class FunctionalMask(ExternalProgramTask):
    filepath = luigi.Parameter()
    output_string = None
    mask_string = None

    def requires(self):
        return MotionCorrectA(self.filepath)

    def output(self):
        self.output_string = add_name(self.input()["output_image"].path, "automask")
        self.mask_string = add_name(self.input()["output_image"].path, "mask")
        return luigi.LocalTarget(self.output_string)

    def program_args(self):
        return [
            "3dAutomask",
            "-apply_prefix",
            self.output_string,
            "-prefix",
            self.mask_string,
            str(self.input()["output_image"].path),
        ]


class AnatRegister(ExternalProgramTask):
    anatpath = luigi.Parameter()
    funcpath = luigi.Parameter()
    output_string = None

    def requires(self):
        return SkullStrip(self.anatpath), FunctionalMask(self.funcpath)

    def output(self):
        self.output_string = str(self.input()[1].path).split(".")[0] + ".mat"
        return luigi.LocalTarget(self.output_string)

    def program_args(self):
        anat_file, func_file = self.input()
        return [
            "flirt",
            "-in",
            str(func_file.path),
            "-ref",
            str(anat_file.path),
            "-omat",
            self.output_string,
            "-cost",
            "corratio",
            "-dof",
            "6",
            "-interp",
            "trilinear",
        ]


class Segment(ExternalProgramTask):
    filepath = luigi.Parameter()
    output_pattern = None
    whitematter_path = None

    def requires(self):
        return SkullStrip(self.filepath)

    def output(self):
        output_folder = os.sep.join(self.input().path.split(os.sep)[0:-1])
        self.output_pattern = os.path.join(output_folder, "segment")
        self.whitematter_path = os.path.join(output_folder, "segment_prob_2.nii.gz")
        return luigi.LocalTarget(self.whitematter_path)

    def program_args(self):
        return [
            "fast",
            "-t",
            "1",
            "-o",
            self.output_pattern,
            "-p",
            "-g",
            "-S",
            "1",
            str(self.input().path),
        ]


class WhiteMatterBinarize(ExternalProgramTask):
    filepath = luigi.Parameter()
    output_string = None

    def requires(self):
        return Segment(self.filepath)

    def output(self):
        self.output_string = add_name(str(self.input().path), "maths")
        return luigi.LocalTarget(self.output_string)

    def program_args(self):
        return ["fslmaths", self.filepath, "-thr", "0.5", "-bin", self.output_string]

    pass


class WMAnatRegister(ExternalProgramTask):
    anatpath = luigi.Parameter()
    funcpath = luigi.Parameter()
    output_string = None

    def requires(self):
        return (
            SkullStrip(self.anatpath),
            WhiteMatterBinarize(self.anatpath),
            FunctionalMask(self.funcpath),
            AnatRegister(self.anatpath, self.funcpath),
        )

    def output(self):
        self.output_string = str(self.input()[1].path).split(".")[0] + "_wmreg.mat"
        return luigi.LocalTarget(self.output_string)

    def program_args(self):
        anat_file, wm_binary, func_file, register_matrix = self.input()
        return [
            "flirt",
            "-in",
            str(func_file.path),
            "-ref",
            str(anat_file.path),
            "-omat",
            self.output_string,
            "-cost",
            "bbr",
            "-wmseg",
            str(wm_binary.path),
            "-dof",
            "6",
            "-init",
            str(register_matrix.path),
            "-schedule",
            "/usr/share/fsl/5.0/etc/flirtsch/bbr.sch",
        ]


class ConvertFSLAffine(ExternalProgramTask):
    anatpath = luigi.Parameter()
    funcpath = luigi.Parameter()
    output_folder = None

    def requires(self):
        return (
            SkullStrip(self.anatpath),
            MeanMasked(self.funcpath),
            WMAnatRegister(self.anatpath, self.funcpath),
        )

    def output(self):
        self.output_folder = os.path.sep.join(
            str(self.input()[1].path).split(os.path.sep)[0:-1]
        )
        return luigi.LocalTarget(os.path.join(self.output_folder, "affine.txt"))

    def program_args(self):
        anat_image, func_image, register_mat = self.input()
        return [
            "c3d_affine_tool",
            "-ref",
            str(anat_image.path),
            "-src",
            str(func_image.path),
            str(register_mat.path),
            "-fsl2ras",
            "-oitk",
            os.path.join(self.output_folder, "affine.txt"),
            "&&",
            "sed",
            "s/MatrixOffsetTransformBase_double_3_3/AffineTransform_double_3_3/",
            "-i",
            os.path.join(self.output_folder, "affine.txt"),
        ]


class ApplyAntsWarp(ExternalProgramTask):
    anatpath = luigi.Parameter()
    funcpath = luigi.Parameter()
    output_string = None

    def requires(self):
        return (
            AntsRegister(self.anatpath),
            FunctionalMask(self.funcpath),
            ConvertFSLAffine(self.anatpath, self.funcpath),
        )

    def output(self):
        self.output_string = add_name(str(self.input()[1].path), "antswarp")
        return luigi.LocalTarget(self.output_string)

    def program_args(self):
        ants_output, functional, affine = self.input()
        return shlex.split(
                (
                    "antsApplyTransforms "
                    "--default-value 0 "
                    "--dimensionality 3 "
                    "--float 0 "
                    "--input {filepath} "
                    "--input-image-type 3 "
                    "--interpolation Linear "
                    "--output {outpath} "
                    "--reference-image /usr/share/fsl/5.0/data/standard/MNI152_T1_2mm_brain.nii.gz "
                    "--transform {transform3} "
                    "--transform {transform2} "
                    "--transform {transform1} "
                    "--transform {transform0} "
                    "--transform {affine}"
                ).format(
                    filepath=str(functional.path),
                    outpath=self.output_string,
                    transform3=str(ants_output["transform3"]),
                    transform2=str(ants_output["transform2"]),
                    transform1=str(ants_output["transform1"]),
                    transform0=str(ants_output["transform0"]),
                    affine=str(affine.path),
                )
            )



class ConvertToAnts(ExternalProgramTask):
    anatpath = luigi.Parameter()
    funcpath = luigi.Parameter()
    output_file = None

    def requires(self):
        return ConvertFSLAffine(self.anatpath, self.funcpath)

    def output(self):
        self.output_file = str(self.input().path).replace("affine", "affine_conv")
        return luigi.LocalTarget(self.output_file)

    def program_args(self):
        input_file = self.input()
        return [
            "sed",
            "s/MatrixOffsetTransformBase_double_3_3/AffineTransform_double_3_3/;w "
            + self.output_file,
            str(input_file.path),
        ]


class MeanImage(ExternalProgramTask):
    filepath = luigi.Parameter()
    outpath_string = None

    def requires(self):
        return Resample(self.filepath)

    def output(self):
        self.outpath_string = add_name(str(self.input()["output_image"].path), "mean")
        return luigi.LocalTarget(self.outpath_string)

    def program_args(self):
        inpath = str(self.input()["output_image"].path)
        return ["3dTstat", "-mean", "-prefix", self.outpath_string, inpath]


class MeanMasked(MeanImage):
    def requires(self):
        return {"output_image": FunctionalMask(self.filepath)}


class MotionCorrect(ExternalProgramTask):
    filepath = luigi.Parameter()

    def requires(self):
        return Resample(self.filepath), MeanImage(self.filepath)

    def output(self):
        image, mean = self.input()
        image = image["output_image"]
        name = str(image.path).split(".")[0]

        output = {
            "input_image": image,
            "input_mean": mean,
            "aff12": luigi.LocalTarget(name + "_aff12.1D"),
            "output1D": luigi.LocalTarget(name + "_resample.1D"),
            "maxdisp1D": luigi.LocalTarget(name + "_max_displacement.1D"),
            "output_image": luigi.LocalTarget(name + "_volreg.nii.gz"),
        }
        return output

    def program_args(self):
        out_dict = self.output()
        return shlex.split(
            "3dvolreg -Fourier -twopass -1Dfile {output1D} "
            "-1Dmatrix_save {output_affine} "
            "-prefix {outpath} "
            "-base {meanpath} "
            "-zpad 4 "
            "-maxdisp1D {output_max_displacement} {filepath}".format(
                output1D=str(out_dict["output1D"].path),
                output_affine=str(out_dict["aff12"].path),
                outpath=str(out_dict["output_image"].path),
                meanpath=str(out_dict["input_mean"].path),
                output_max_displacement=str(out_dict["maxdisp1D"].path),
                filepath=str(out_dict["input_image"].path),
            )
        )


class MeanMotionCorrect(MeanImage):
    def requires(self):
        return MotionCorrect(self.filepath)


class MotionCorrectA(MotionCorrect):
    def requires(self):
        return MotionCorrect(self.filepath), MeanMotionCorrect(self.filepath)


class SkullStrip(ExternalProgramTask):
    filepath = luigi.Parameter()

    def requires(self):
        return Resample(self.filepath)

    def output(self):
        return luigi.LocalTarget(
            add_name(str(self.input()["output_image"].path), "skullstrip")
        )

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
            "transform0": input_folder
            / "transform0DerivedInitialMovingTranslation.mat",
            "transform1": input_folder / "transform1Rigid.mat",
            "transform2": input_folder / "transform2Affine.mat",
            "transform3": input_folder / "transform3Warp.nii.gz",
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
            "--winsorize-image-intensities [0.01,0.99] -v".format(
                filepath=inpath, outpath=self.input_folder
            )
        )


def add_name(filepath, task_name="task"):
    filepath_string = str(filepath)
    out_path = ".".join(
        [
            filepath_string.split(".")[0] + "_" + task_name,
            filepath_string.split(".", 1)[1],
        ]
    )
    return out_path


if __name__ == "__main__":
    luigi.run()
