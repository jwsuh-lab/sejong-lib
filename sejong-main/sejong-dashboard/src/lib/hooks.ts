"use client";

import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  fetchSummary,
  fetchCountries,
  fetchCategories,
  fetchDocuments,
  fetchPipelineStatus,
  fetchQualityReport,
  fetchSettings,
  fetchSites,
  createSite,
  updateSite,
  deleteSite,
  fetchPipelineHistory,
  runPipeline,
  runPipelineStep,
  stopPipeline,
  updateSetting,
  type DocumentQueryParams,
  type SiteQueryParams,
} from "./api";
import type { SiteData } from "./types";

export function useSummary() {
  return useQuery({
    queryKey: ["summary"],
    queryFn: fetchSummary,
    refetchInterval: 10000,
  });
}

export function useCountries() {
  return useQuery({
    queryKey: ["countries"],
    queryFn: fetchCountries,
  });
}

export function useCategories() {
  return useQuery({
    queryKey: ["categories"],
    queryFn: fetchCategories,
  });
}

export function useDocuments(params: DocumentQueryParams) {
  return useQuery({
    queryKey: ["documents", params],
    queryFn: () => fetchDocuments(params),
  });
}

export function usePipelineStatus() {
  return useQuery({
    queryKey: ["pipeline-status"],
    queryFn: fetchPipelineStatus,
    refetchInterval: 3000,
  });
}

export function usePipelineHistory() {
  return useQuery({
    queryKey: ["pipeline-history"],
    queryFn: fetchPipelineHistory,
  });
}

export function useQualityReport() {
  return useQuery({
    queryKey: ["quality-report"],
    queryFn: fetchQualityReport,
  });
}

export function useSettings() {
  return useQuery({
    queryKey: ["settings"],
    queryFn: fetchSettings,
  });
}

export function useRunPipeline() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (steps?: number[]) => runPipeline(steps),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pipeline-status"] });
      qc.invalidateQueries({ queryKey: ["summary"] });
    },
  });
}

export function useRunPipelineStep() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (step: number) => runPipelineStep(step),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pipeline-status"] });
    },
  });
}

export function useStopPipeline() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => stopPipeline(),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pipeline-status"] });
    },
  });
}

export function useUpdateSetting() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key, value }: { key: string; value: string }) =>
      updateSetting(key, value),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["settings"] });
    },
  });
}

// ─── Sites ───

export function useSites(params: SiteQueryParams) {
  return useQuery({
    queryKey: ["sites", params],
    queryFn: () => fetchSites(params),
  });
}

export function useCreateSite() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (data: Partial<SiteData>) => createSite(data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sites"] });
      qc.invalidateQueries({ queryKey: ["summary"] });
    },
  });
}

export function useUpdateSite() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: number; data: Partial<SiteData> }) =>
      updateSite(id, data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sites"] });
    },
  });
}

export function useDeleteSite() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: number) => deleteSite(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sites"] });
      qc.invalidateQueries({ queryKey: ["summary"] });
    },
  });
}
