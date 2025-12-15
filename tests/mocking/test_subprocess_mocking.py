"""Tests for mocking NCO subprocess calls.

This module tests that NCO (netCDF Operators) subprocess calls, specifically
ncks, can be properly mocked. This allows testing of model file slicing
operations without requiring NCO tools to be installed.
"""

import os
import pytest
from pathlib import Path
from unittest.mock import patch, Mock
import subprocess


class TestNcksSubprocessMocking:
    """Test mocking of ncks (netCDF Kitchen Sink) subprocess calls."""
    
    def test_mock_ncks_time_slice(self, tmp_path):
        """Test mocking ncks time dimension slicing."""
        # Create mock input file
        input_file = tmp_path / "model_input.nc"
        input_file.write_text("mock netcdf data")
        output_file = tmp_path / "model_slice.nc"
        
        # Mock successful ncks call
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)
            
            # Simulate ncks call: ncks -d time,5 input.nc output.nc
            ncks_cmd = ["ncks", "-d", "time,5", str(input_file), str(output_file)]
            result = subprocess.run(ncks_cmd, check=True)
            
            # Verify mock was called correctly
            mock_run.assert_called_once_with(ncks_cmd, check=True)
            assert result.returncode == 0
    
    def test_mock_ncks_with_check_true(self, tmp_path):
        """Test that ncks mock respects check=True parameter."""
        input_file = tmp_path / "input.nc"
        output_file = tmp_path / "output.nc"
        
        with patch('subprocess.run') as mock_run:
            # Mock a failed ncks call
            mock_run.side_effect = subprocess.CalledProcessError(1, "ncks")
            
            # Verify that check=True causes exception
            with pytest.raises(subprocess.CalledProcessError):
                ncks_cmd = ["ncks", "-d", "time,0", str(input_file), str(output_file)]
                subprocess.run(ncks_cmd, check=True)
    
    def test_mock_ncks_multiple_dimensions(self, tmp_path):
        """Test mocking ncks with multiple dimension slices."""
        input_file = tmp_path / "model.nc"
        output_file = tmp_path / "subset.nc"
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)
            
            # ncks with multiple dimensions
            ncks_cmd = [
                "ncks", 
                "-d", "time,10",
                "-d", "depth,0,5",
                str(input_file), 
                str(output_file)
            ]
            subprocess.run(ncks_cmd, check=True)
            
            # Verify call
            mock_run.assert_called_once()
            args = mock_run.call_args[0][0]
            assert "ncks" in args
            assert "-d" in args
            assert "time,10" in args
    
    def test_mock_ncks_with_output_creation(self, tmp_path):
        """Test mocking ncks that creates output file."""
        input_file = tmp_path / "input.nc"
        input_file.write_text("mock input netcdf")
        output_file = tmp_path / "output.nc"
        
        def create_output_file(*args, **kwargs):
            # Side effect: create the output file
            output_file.write_text("mock output netcdf")
            return Mock(returncode=0)
        
        with patch('subprocess.run', side_effect=create_output_file):
            ncks_cmd = ["ncks", "-d", "time,0", str(input_file), str(output_file)]
            subprocess.run(ncks_cmd, check=True)
            
            # Verify output file was created by side effect
            assert output_file.exists()
            assert output_file.read_text() == "mock output netcdf"
    
    def test_mock_ncks_failure_handling(self, tmp_path):
        """Test handling ncks execution failures."""
        input_file = tmp_path / "missing.nc"
        output_file = tmp_path / "output.nc"
        
        with patch('subprocess.run') as mock_run:
            # Mock ncks failure (e.g., invalid dimension)
            mock_run.side_effect = subprocess.CalledProcessError(
                1, 
                "ncks",
                stderr="ncks: ERROR dimension time not found"
            )
            
            with pytest.raises(subprocess.CalledProcessError):
                ncks_cmd = ["ncks", "-d", "time,999", str(input_file), str(output_file)]
                subprocess.run(ncks_cmd, check=True)


class TestSubprocessRunVsPopen:
    """Test mocking both subprocess.run and subprocess.Popen."""
    
    def test_mock_subprocess_run(self):
        """Test mocking subprocess.run for ncks."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
            
            result = subprocess.run(["ncks", "--version"], capture_output=True)
            
            mock_run.assert_called_once()
            assert result.returncode == 0
    
    def test_mock_subprocess_popen(self):
        """Test mocking subprocess.Popen for perfect_model_obs."""
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.wait.return_value = None
        
        with patch('subprocess.Popen', return_value=mock_process) as mock_popen:
            process = subprocess.Popen(["perfect_model_obs"])
            process.wait()
            
            mock_popen.assert_called_once()
            assert process.returncode == 0
    
    def test_mock_both_run_and_popen(self, tmp_path):
        """Test mocking both run and Popen in same test (workflow scenario)."""
        # Mock ncks (subprocess.run)
        mock_run_return = Mock(returncode=0)
        
        # Mock perfect_model_obs (subprocess.Popen)
        mock_popen_return = Mock()
        mock_popen_return.returncode = 0
        mock_popen_return.wait.return_value = None
        
        with patch('subprocess.run', return_value=mock_run_return) as mock_run:
            with patch('subprocess.Popen', return_value=mock_popen_return) as mock_popen:
                # First: ncks call
                ncks_result = subprocess.run(["ncks", "-d", "time,0", "in.nc", "out.nc"], check=True)
                
                # Second: perfect_model_obs call
                logfile = tmp_path / "pmo.log"
                with open(logfile, "a") as log:
                    pmo_process = subprocess.Popen(
                        ["perfect_model_obs"],
                        stdout=log,
                        stderr=subprocess.STDOUT
                    )
                    pmo_process.wait()
                
                # Verify both were called
                mock_run.assert_called_once()
                mock_popen.assert_called_once()
                assert ncks_result.returncode == 0
                assert pmo_process.returncode == 0


class TestSubprocessMockingEdgeCases:
    """Test edge cases in subprocess mocking."""
    
    def test_mock_with_environment_variables(self):
        """Test mocking subprocess with custom environment."""
        custom_env = {"CUSTOM_VAR": "value"}
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)
            
            subprocess.run(["ncks", "--version"], env=custom_env)
            
            _, kwargs = mock_run.call_args
            assert kwargs['env'] == custom_env
    
    def test_mock_with_timeout(self):
        """Test mocking subprocess with timeout parameter."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)
            
            subprocess.run(["ncks", "-d", "time,0", "in.nc", "out.nc"], timeout=30)
            
            _, kwargs = mock_run.call_args
            assert kwargs['timeout'] == 30
    
    def test_mock_subprocess_timeout_exception(self):
        """Test mocking subprocess timeout exception."""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("ncks", 30)
            
            with pytest.raises(subprocess.TimeoutExpired):
                subprocess.run(["ncks", "-d", "time,0", "in.nc", "out.nc"], 
                             timeout=30, check=True)
    
    def test_mock_capture_output(self):
        """Test mocking subprocess with output capture."""
        mock_output = "ncks version 5.0.0"
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = Mock(
                returncode=0,
                stdout=mock_output,
                stderr=""
            )
            
            result = subprocess.run(["ncks", "--version"], capture_output=True, text=True)
            
            assert result.stdout == mock_output
            assert result.stderr == ""
            assert result.returncode == 0


class TestWorkflowSubprocessSequence:
    """Test realistic workflow subprocess call sequences."""
    
    def test_workflow_subprocess_sequence(self, tmp_path):
        """Test a complete workflow sequence of subprocess calls."""
        # This mimics the actual workflow: ncks -> perfect_model_obs
        
        def ncks_side_effect(*args, **kwargs):
            # Create output file when ncks is called
            cmd = args[0]
            if "ncks" in cmd and len(cmd) >= 5:
                output_path = cmd[-1]
                Path(output_path).write_text("mock sliced netcdf")
            return Mock(returncode=0)
        
        def pmo_side_effect(*args, **kwargs):
            # Create obs_seq.final when perfect_model_obs is called
            cwd = kwargs.get('cwd', os.getcwd())
            obs_final = Path(cwd) / "obs_seq.final"
            obs_final.write_text("mock observations")
            
            mock_proc = Mock()
            mock_proc.returncode = 0
            mock_proc.wait.return_value = None
            return mock_proc
        
        with patch('subprocess.run', side_effect=ncks_side_effect):
            with patch('subprocess.Popen', side_effect=pmo_side_effect):
                # Step 1: ncks extracts time slice
                input_nc = tmp_path / "model.nc"
                input_nc.write_text("full model data")
                slice_nc = tmp_path / "model_slice.nc"
                
                ncks_cmd = ["ncks", "-d", "time,5", str(input_nc), str(slice_nc)]
                subprocess.run(ncks_cmd, check=True)
                
                assert slice_nc.exists()
                
                # Step 2: perfect_model_obs processes observations
                work_dir = str(tmp_path)
                logfile = tmp_path / "pmo.log"
                
                with open(logfile, "a") as log:
                    process = subprocess.Popen(
                        ["perfect_model_obs"],
                        cwd=work_dir,
                        stdout=log,
                        stderr=subprocess.STDOUT
                    )
                    process.wait()
                
                obs_final = tmp_path / "obs_seq.final"
                assert obs_final.exists()
                assert process.returncode == 0
